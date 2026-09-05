use std::env;
use std::fs::File;
use std::io::{self, BufWriter, Write as _};
use std::path::Path;

fn write_kw_map(file: &mut BufWriter<File>) -> io::Result<()> {
    let kws = [
        "BYTEORD",
        "MODE",
        "CYT",
        "TOT",
        "BEGINANALYSIS",
        "BEGINDATA",
        "BEGINSTEXT",
        "ENDANALYSIS",
        "ENDDATA",
        "ENDSTEXT",
        "TIMESTEP",
        "ABRT",
        "BTIM",
        "CYTSN",
        "COM",
        "CELLS",
        "DATATYPE",
        "DATE",
        "ETIM",
        "EXP",
        "FIL",
        "GATING",
        "INST",
        "LOST",
        "NEXTDATA",
        "OP",
        "PAR",
        "PROJ",
        "SMNO",
        "SRC",
        "SYS",
        "TR",
        "LAST_MODIFIER",
        "ORIGINALITY",
        "LAST_MODIFIED",
        "PLATEID",
        "PLATENAME",
        "WELLID",
        "SPILLOVER",
        "VOL",
        "CARRIERID",
        "CARRIERTYPE",
        "LOCATIONID",
        "BEGINDATETIME",
        "ENDDATETIME",
        "UNSTAINEDCENTERS",
        "UNSTAINEDINFO",
        "FLOWRATE",
        "GATE",
        "CSMODE",
        "CSTOT",
        "CSVBITS",
        "UNICODE",
        "COMP",
    ];

    for v in kws {
        writeln!(file, "pub const {v}: &NEStr = ne_str!(\"${v}\");")?;
        writeln!(file, "pub const {v}_KW: &NEStr = ne_str!(\"{v}\");")?;
    }

    Ok(())
}

fn write_meas_kw_map(file: &mut BufWriter<File>) -> io::Result<()> {
    let kws = [
        ("E", true),
        ("L", false),
        ("N", true),
        ("B", false),
        ("F", true),
        ("O", false),
        ("P", true),
        ("R", true),
        ("S", true),
        ("T", true),
        ("V", true),
        ("G", false),
        ("D", false),
        ("CALIBRATION", false),
        ("FEATURE", false),
        ("TYPE", false),
        ("DATATYPE", false),
        ("ANALYTE", false),
        ("TAG", false),
        ("DET", false),
    ];

    for (v, also_gate) in kws {
        writeln!(file, "pub const PN{v}: &NEStr = ne_str!(\"$Pn{v}\");")?;
        writeln!(file, "pub const {v}_KW_SUFFIX: &NEStr = ne_str!(\"{v}\");")?;
        if also_gate {
            writeln!(file, "pub const GM{v}: &NEStr = ne_str!(\"$Gm{v}\");")?;
        }
    }
    Ok(())
}

fn main() {
    let path = Path::new(&env::var("OUT_DIR").unwrap()).join("kw_strs.rs");
    let mut file = BufWriter::new(File::create(&path).unwrap());
    write_kw_map(&mut file).unwrap();
    write_meas_kw_map(&mut file).unwrap();
}

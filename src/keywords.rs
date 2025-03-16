const ABRT: &str = "$ABRT";
const BYTEORD: &str = "$BYTEORD";
const BEGINANALYSIS: &str = "$BEGINANALYSIS";
const BEGINDATA: &str = "$BEGINDATA";
const BEGINSTEXT: &str = "$BEGINSTEXT";
const CELLS: &str = "$CELLS";
const COM: &str = "$COM";
const CYT: &str = "$CYT";
const DATATYPE: &str = "$DATATYPE";
const ENDANALYSIS: &str = "$ENDANALYSIS";
const ENDDATA: &str = "$ENDDATA";
const ENDSTEXT: &str = "$ENDSTEXT";
const EXP: &str = "$EXP";
const FIL: &str = "$FIL";
const INST: &str = "$INST";
const LOST: &str = "$LOST";
const MODE: &str = "$MODE";
const NEXTDATA: &str = "$NEXTDATA";
const OP: &str = "$OP";
const PAR: &str = "$PAR";
const PROJ: &str = "$PROJ";
const SMNO: &str = "$SMNO";
const SRC: &str = "$SRC";
const SYS: &str = "$SYS";
const TOT: &str = "$TOT";
const TR: &str = "$TR";

const BTIM: &str = "$BTIM"; // deprecated since 3.2
const ETIM: &str = "$ETIM"; // deprecated since 3.2
const DATE: &str = "$ETIM"; // deprecated since 3.2

const CYTSN: &str = "$CYTSN"; // since 3.0
const TIMESTEP: &str = "$TIMESTEP"; // since 3.0
const UNICODE: &str = "$UNICODE"; // 3.0 only

const VOL: &str = "$VOL"; // since 3.1
const FLOWRATE: &str = "$FLOWRATE"; // since 3.1

const COMP: &str = "$COMP"; // 3.0 only
const SPILLOVER: &str = "$SPILLOVER"; // since 3.1
const PLATEID: &str = "$PLATEID"; // since 3.1
const PLATENAME: &str = "$PLATENAME"; // since 3.1
const WELLID: &str = "$WELLID"; // since 3.1
const LAST_MODIFIER: &str = "$LAST_MODIFIER"; // since 3.1
const LAST_MODIFIED: &str = "$LAST_MODIFIED"; // since 3.1
const ORIGINALITY: &str = "$ORIGINALITY"; // since 3.1

const BEGINDATETIME: &str = "$BEGINDATETIME"; // since 3.2
const CARRIERID: &str = "$CARRIERID"; // since 3.2
const CARRIERTYPE: &str = "$CARRIERTYPE"; // since 3.2
const ENDDATETIME: &str = "$ENDDATETIME"; // since 3.2
const LOCATIONID: &str = "$LOCATIONID"; // since 3.2
const UNSTAINEDCENTERS: &str = "$UNSTAINEDCENTERS"; // since 3.2
const UNSTAINEDINFO: &str = "$UNSTAINEDINFO"; // since 3.2

// const fn sub_pn(suffix: &str) -> &str {
//     (String::from("Pn") + suffix).as_str()
// }

// const PnN: &str = sub_pn("N");

use fireflow_core::api::*;
use fireflow_core::config;
use fireflow_core::error::*;
use fireflow_core::validated::datepattern::DatePattern;
use fireflow_core::validated::keys::NonStdMeasPattern;
use fireflow_core::validated::pattern::*;

use clap::{arg, value_parser, Command};
use serde::ser::Serialize;
use std::fmt::Display;
use std::path::PathBuf;

fn print_json<T: Serialize>(j: &T) {
    println!("{}", serde_json::to_string(j).unwrap());
}

pub fn print_parsed_data(s: &StdDatasetOutput, _delim: &str) {
    let df = s.dataset.standardized.core.as_data();
    let nrows = df.nrows();
    let cols: Vec<_> = df.iter_columns().collect();
    let ncols = cols.len();
    if ncols == 0 {
        return;
    }
    let mut ns = s.dataset.standardized.core.shortnames().into_iter();
    print!("{}", ns.next().unwrap());
    for n in ns {
        print!("\t{n}");
    }
    for r in 0..nrows {
        println!();
        print!("{}", cols[0].pos_to_string(r));
        (1..ncols)
            .map(|c| print!("\t{}", cols[c].pos_to_string(r)))
            .collect()
    }
}

// TODO use warnings_are_errors flag
fn handle_warnings<X, W>(t: Terminal<X, W>) -> X
where
    W: Display,
{
    t.resolve(print_warnings).0
}

fn print_warnings<W>(ws: Vec<W>)
where
    W: Display,
{
    for w in ws {
        eprintln!("WARNING: {}", w)
    }
}

// TODO use warnings_are_errors flag
fn handle_failure<W, E, T>(f: TerminalFailure<W, E, T>)
where
    E: Display,
    T: Display,
    W: Display,
{
    f.resolve(print_warnings, |e| match e {
        Failure::Single(t) => eprintln!("ERROR: {t}"),
        Failure::Many(t, es) => {
            eprintln!("TOPLEVEL ERROR: {t}");
            for e in *es {
                eprintln!("  ERROR: {e}");
            }
        }
    });
}

fn handle_failure_nowarn<E, T>(f: TerminalFailure<(), E, T>)
where
    E: Display,
    T: Display,
{
    // TODO not DRY
    f.resolve(
        |_| (),
        |e| match e {
            Failure::Single(t) => eprintln!("ERROR: {t}"),
            Failure::Many(t, es) => {
                eprintln!("TOPLEVEL ERROR: {t}");
                for e in *es {
                    eprintln!("  ERROR: {e}");
                }
            }
        },
    );
}

fn main() -> Result<(), ()> {
    let begintext_arg = arg!(--"begintext-delta" [OFFSET] "adjustment for begin TEXT offset")
        .value_parser(value_parser!(i32));
    let endtext_arg = arg!(--"endtext-delta" [OFFSET] "adjustment for end TEXT offset")
        .value_parser(value_parser!(i32));

    let begindata_arg = arg!(--"begindata-delta" [OFFSET] "adjustment for begin DATA offset")
        .value_parser(value_parser!(i32));
    let enddata_arg = arg!(--"enddata-delta" [OFFSET] "adjustment for end DATA offset")
        .value_parser(value_parser!(i32));

    let delim_arg =
        arg!(-d --delimiter [DELIM] "delimiter to use for the table").default_value("\t");

    let repair_offset_spaces_arg =
        arg!(-o --"trim-whitespace" "remove spaces from offset keywords");

    let max_other = arg!(--"max-other" [BYTES] "max number of OTHER segments to parse")
        .value_parser(value_parser!(usize));
    let other_width =
        arg!(--"other-width" [WIDTH] "width of OTHER segments").value_parser(value_parser!(u8));
    let squish_offsets = arg!(--"squish-offsets" "squish DATA/ANALYSIS, offsets that end in 0");
    let allow_negative = arg!(--"allow-negative" "substitute 0 for negative offsets");
    let allow_dup_stext = arg!(--"allow-dup-stext" "only throw warning if STEXT is same as TEXT");
    let ignore_stext = arg!(--"ignore-stext" "ignore STEXT entirely");

    let cmd = Command::new("fireflow")
        .about("read and write FCS files")
        .arg(
            arg!([INPUT_PATH] "input file path")
                .value_parser(value_parser!(PathBuf))
                .required(true)
        )

        .subcommand(
            Command::new("header")
                .about("show header as JSON")
                .arg(&max_other)
                .arg(&other_width)
                .arg(&squish_offsets)
                .arg(&allow_negative)
        )

        .subcommand(
            Command::new("raw")
                .about("show raw keywords as JSON")
                .arg(arg!(-H --header "also show header"))
                .arg(&begintext_arg)
                .arg(&endtext_arg)
                .arg(&repair_offset_spaces_arg)
                .arg(&max_other)
                .arg(&other_width)
                .arg(&squish_offsets)
                .arg(&allow_negative)
                .arg(&allow_dup_stext)
                .arg(&ignore_stext)
        )

        .subcommand(
            Command::new("std")
                .about("dump standardized keywords as JSON")
                .arg(arg!(-H --header "also show header"))
                .arg(arg!(-r --raw "also show raw"))
                .arg(&begintext_arg)
                .arg(&endtext_arg)
                .arg(arg!(-t --"time-name" [NAME] "name of time measurement"))
                .arg(arg!(-T --"ensure-time" "make sure time measurement exists"))
                // .arg(arg!(-l --"ensure-time-linear" "ensure time measurement is linear"))
                // .arg(arg!(-g --"ensure-time-nogain" "ensure time measurement does not have gain"))
                .arg(arg!(-d --"allow-pseudostandard" "allow pseudostandard keywords"))
                .arg(arg!(-D --"disallow-deprecated" "disallow deprecated keywords"))
                .arg(arg!(-p --"date-pattern" [PATTERN] "pattern to use when matching $DATE"))
                .arg(arg!(-P --"ns-meas-pattern" [PATTERN] "pattern used to for nonstandard measurement keywords"))
                .arg(&repair_offset_spaces_arg)
                .arg(&max_other)
                .arg(&other_width)
                .arg(&squish_offsets)
                .arg(&allow_negative)
                .arg(&allow_dup_stext)
                .arg(&ignore_stext)
        )

        .subcommand(
            Command::new("measurements")
                .about("show a table of standardized measurement values")
                .arg(&begintext_arg)
                .arg(&endtext_arg)
                .arg(&delim_arg)
                // TODO this shouldn't be necessary since we aren't reading DATA
                .arg(&repair_offset_spaces_arg)
                .arg(&max_other)
                .arg(&other_width)
                .arg(&squish_offsets)
                .arg(&allow_negative)
                .arg(&allow_dup_stext)
                .arg(&ignore_stext)
        )

        .subcommand(
            Command::new("spillover")
                .about("dump the spillover matrix if present")
                .arg(&begintext_arg)
                .arg(&endtext_arg)
                .arg(&delim_arg)
                // TODO this shouldn't be necessary since we aren't reading DATA
                .arg(&repair_offset_spaces_arg)
                .arg(&max_other)
                .arg(&other_width)
                .arg(&squish_offsets)
                .arg(&allow_negative)
                .arg(&allow_dup_stext)
                .arg(&ignore_stext)
        )

        .subcommand(
            Command::new("data")
                .about("show a table of the DATA segment")
                .arg(&begintext_arg)
                .arg(&endtext_arg)
                .arg(&begindata_arg)
                .arg(&enddata_arg)
                .arg(&repair_offset_spaces_arg)
                .arg(&delim_arg)
                .arg(&max_other)
                .arg(&other_width)
                .arg(&squish_offsets)
                .arg(&allow_negative)
                .arg(&allow_dup_stext)
                .arg(&ignore_stext)
        );

    let args = cmd.get_matches();

    let filepath = args.get_one::<PathBuf>("INPUT_PATH").unwrap();

    // let get_text_delta = |args: &ArgMatches| {
    //     let mut begin = 0;
    //     let mut end = 0;
    //     if let Some(x) = args.get_one("begintext-delta") {
    //         begin = *x
    //     }
    //     if let Some(x) = args.get_one("endtext-delta") {
    //         end = *x
    //     }
    //     config::OffsetCorrection::new(begin, end)
    // };

    match args.subcommand() {
        Some(("header", sargs)) => {
            let mut conf = config::HeaderConfig::default();
            conf = config::HeaderConfig {
                max_other: sargs.get_one::<usize>("max-other").copied(),
                other_width: sargs
                    .get_one::<u8>("other-width")
                    .copied()
                    .map(|x| x.try_into().unwrap())
                    .unwrap_or_default(),
                allow_negative: sargs.get_flag("allow-negative"),
                squish_offsets: sargs.get_flag("squish-offsets"),
                ..conf
            };
            fcs_read_header(filepath, &conf)
                .map(|h| print_json(&h.inner()))
                .map_err(handle_failure_nowarn)
        }

        Some(("raw", sargs)) => {
            let mut conf = config::RawTextReadConfig::default();
            conf.header = config::HeaderConfig {
                max_other: sargs.get_one::<usize>("max-other").copied(),
                other_width: sargs
                    .get_one::<u8>("other-width")
                    .copied()
                    .map(|x| x.try_into().unwrap())
                    .unwrap_or_default(),
                allow_negative: sargs.get_flag("allow-negative"),
                squish_offsets: sargs.get_flag("squish-offsets"),
                ..conf.header
            };
            conf = config::RawTextReadConfig {
                trim_value_whitespace: sargs.get_flag("trim-whitespace"),
                allow_duplicated_stext: sargs.get_flag("allow-dup-stext"),
                ignore_supp_text: sargs.get_flag("ignore-stext"),
                ..conf
            };
            fcs_read_raw_text(filepath, &conf)
                .map(handle_warnings)
                .map(|raw| print_json(&raw))
                .map_err(handle_failure)
        }

        Some(("spillover", sargs)) => {
            let mut conf = config::StdTextReadConfig::default();
            conf.raw.header = config::HeaderConfig {
                max_other: sargs.get_one::<usize>("max-other").copied(),
                other_width: sargs
                    .get_one::<u8>("other-width")
                    .copied()
                    .map(|x| x.try_into().unwrap())
                    .unwrap_or_default(),
                allow_negative: sargs.get_flag("allow-negative"),
                squish_offsets: sargs.get_flag("squish-offsets"),
                ..conf.raw.header
            };
            // get_text_delta(sargs);
            conf.raw.trim_value_whitespace = sargs.get_flag("trim-whitespace");
            conf.raw.allow_duplicated_stext = sargs.get_flag("allow-dup-stext");
            let delim = sargs.get_one::<String>("delimiter").unwrap();

            fcs_read_std_text(filepath, &conf)
                .map(handle_warnings)
                .map(|std| std.standardized.print_spillover_table(delim))
                .map_err(handle_failure)
        }

        Some(("measurements", sargs)) => {
            let mut conf = config::StdTextReadConfig::default();
            conf.raw.header = config::HeaderConfig {
                max_other: sargs.get_one::<usize>("max-other").copied(),
                other_width: sargs
                    .get_one::<u8>("other-width")
                    .copied()
                    .map(|x| x.try_into().unwrap())
                    .unwrap_or_default(),
                allow_negative: sargs.get_flag("allow-negative"),
                squish_offsets: sargs.get_flag("squish-offsets"),
                ..conf.raw.header
            };
            // get_text_delta(sargs);
            conf.raw.trim_value_whitespace = sargs.get_flag("trim-whitespace");
            conf.raw.allow_duplicated_stext = sargs.get_flag("allow-dup-stext");
            conf.raw.ignore_supp_text = sargs.get_flag("ignore-stext");
            let delim = sargs.get_one::<String>("delimiter").unwrap();

            fcs_read_std_text(filepath, &conf)
                .map(handle_warnings)
                .map(|std| std.standardized.print_meas_table(delim))
                .map_err(handle_failure)
        }

        Some(("std", sargs)) => {
            let mut conf = config::StdTextReadConfig::default();

            conf.raw.header = config::HeaderConfig {
                max_other: sargs.get_one::<usize>("max-other").copied(),
                other_width: sargs
                    .get_one::<u8>("other-width")
                    .copied()
                    .map(|x| x.try_into().unwrap())
                    .unwrap_or_default(),
                allow_negative: sargs.get_flag("allow-negative"),
                squish_offsets: sargs.get_flag("squish-offsets"),
                ..conf.raw.header
            };
            // get_text_delta(sargs);

            // TODO refactor
            if let Some(d) = sargs.get_one::<String>("date-pattern").cloned() {
                conf.raw.date_pattern = Some(d.parse::<DatePattern>().unwrap());
            }

            if let Some(m) = sargs.get_one::<String>("ns-meas-pattern").cloned() {
                conf.nonstandard_measurement_pattern =
                    Some(m.parse::<NonStdMeasPattern>().unwrap());
            }

            if let Some(m) = sargs.get_one::<String>("time-name").cloned() {
                conf.time.pattern = Some(m.parse::<TimePattern>().unwrap());
            }

            conf.time.allow_missing = sargs.get_flag("ensure-time");
            conf.raw.allow_duplicated_stext = sargs.get_flag("allow-dup-stext");
            conf.raw.ignore_supp_text = sargs.get_flag("ignore-stext");
            // conf.time.allow_nonlinear_scale = sargs.get_flag("ensure-time-linear");
            // conf.time.allow_nontime_keywords = sargs.get_flag("ensure-time-nogain");
            conf.allow_pseudostandard = sargs.get_flag("allow-pseudostandard");
            conf.disallow_deprecated = sargs.get_flag("disallow-deprecated");
            conf.raw.trim_value_whitespace = sargs.get_flag("trim-whitespace");

            fcs_read_std_text(filepath, &conf)
                .map(handle_warnings)
                .map(|std| {
                    print_json(&std.standardized);
                })
                .map_err(handle_failure)
        }

        Some(("data", sargs)) => {
            let mut conf = config::DataReadConfig::default();

            conf.standard.raw.header = config::HeaderConfig {
                max_other: sargs.get_one::<usize>("max-other").copied(),
                other_width: sargs
                    .get_one::<u8>("other-width")
                    .copied()
                    .map(|x| x.try_into().unwrap())
                    .unwrap_or_default(),
                allow_negative: sargs.get_flag("allow-negative"),
                squish_offsets: sargs.get_flag("squish-offsets"),
                ..conf.standard.raw.header
            };

            // get_text_delta(sargs);
            // TODO add DATA delta adjust
            conf.standard.raw.allow_duplicated_stext = sargs.get_flag("allow-dup-stext");
            conf.standard.raw.ignore_supp_text = sargs.get_flag("ignore-stext");
            conf.standard.raw.trim_value_whitespace = sargs.get_flag("trim-whitespace");
            let delim = sargs.get_one::<String>("delimiter").unwrap();

            fcs_read_std_dataset(filepath, &conf)
                .map(handle_warnings)
                .map(|res| print_parsed_data(&res, delim))
                .map_err(handle_failure)
        }

        _ => Ok(()),
    }
}

// TODO gating parameters not added (yet)

mod api;
mod keywords;
mod numeric;

use clap::{arg, value_parser, ArgMatches, Command};
use serde::ser::Serialize;
use std::path::PathBuf;

fn print_json<T: Serialize>(j: &T) {
    println!("{}", serde_json::to_string(j).unwrap());
}

fn main() {
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
        arg!(-o --"repair-offset-spaces" "remove spaces from offset keywords");

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
        )

        .subcommand(
            Command::new("raw")
                .about("show raw keywords as JSON")
                .arg(arg!(-H --header "also show header"))
                .arg(&begintext_arg)
                .arg(&endtext_arg)
        )

        .subcommand(
            Command::new("std")
                .about("dump standardized keywords as JSON")
                .arg(arg!(-H --header "also show header"))
                .arg(arg!(-r --raw "also show raw"))
                .arg(&begintext_arg)
                .arg(&endtext_arg)
                .arg(arg!(-t --"time-name" [NAME] "name of time channel"))
                .arg(arg!(-T --"ensure-time" "make sure time channel exists"))
                .arg(arg!(-l --"ensure-time-linear" "ensure time channel is linear"))
                .arg(arg!(-g --"ensure-time-nogain" "ensure time channel does not have gain"))
                .arg(arg!(-d --"disallow-deviant" "disallow deviant keywords"))
                .arg(arg!(-n --"disallow-nonstandard" "disallow nonstandard keywords"))
                .arg(arg!(-D --"disallow-deprecated" "disallow deprecated keywords"))
                .arg(arg!(-p --"date-pattern" [PATTERN] "pattern to use when matching $DATE"))
                .arg(arg!(-P --"ns-meas-pattern" [PATTERN] "pattern used to for nonstandard measurement keywords"))
                .arg(&repair_offset_spaces_arg)
        )

        .subcommand(
            Command::new("measurements")
                .about("show a table of standardized measurement values")
                .arg(&begintext_arg)
                .arg(&endtext_arg)
                .arg(&delim_arg)
                // TODO this shouldn't be necessary since we aren't reading DATA
                .arg(&repair_offset_spaces_arg)
        )

        .subcommand(
            Command::new("spillover")
                .about("dump the spillover matrix if present")
                .arg(&begintext_arg)
                .arg(&endtext_arg)
                .arg(&delim_arg)
                // TODO this shouldn't be necessary since we aren't reading DATA
                .arg(&repair_offset_spaces_arg)
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
        );

    let args = cmd.get_matches();

    let filepath = args.get_one::<PathBuf>("INPUT_PATH").unwrap();
    let mut conf = api::Reader::default();

    let mut get_text_delta = |args: &ArgMatches| {
        if let Some(x) = args.get_one("begintext-delta") {
            conf.text.raw.starttext_delta = *x
        }
        if let Some(x) = args.get_one("endtext-delta") {
            conf.text.raw.endtext_delta = *x
        }
    };

    match args.subcommand() {
        Some(("header", _)) => {
            let header = api::read_fcs_header(filepath).unwrap();
            print_json(&header);
        }

        Some(("raw", sargs)) => {
            get_text_delta(sargs);
            let (header, raw) = api::read_fcs_raw_text(filepath, &conf).unwrap();
            if sargs.get_flag("header") {
                print_json(&header);
            }
            print_json(&raw);
        }

        Some(("spillover", sargs)) => {
            get_text_delta(sargs);
            conf.text.raw.repair_offset_spaces = sargs.get_flag("repair-offset-spaces");
            let delim = sargs.get_one::<String>("delimiter").unwrap();

            match api::read_fcs_text(filepath, &conf).unwrap() {
                Err(err) => err.print(),
                Ok(res) => res.standard.print_spillover_table(delim),
            }
        }

        Some(("measurements", sargs)) => {
            get_text_delta(sargs);
            conf.text.raw.repair_offset_spaces = sargs.get_flag("repair-offset-spaces");
            let delim = sargs.get_one::<String>("delimiter").unwrap();

            match api::read_fcs_text(filepath, &conf).unwrap() {
                Err(err) => err.print(),
                Ok(res) => res.standard.print_meas_table(delim),
            }
        }

        Some(("std", sargs)) => {
            get_text_delta(sargs);

            conf.text.time_shortname = sargs.get_one::<String>("time-name").cloned();
            conf.text.raw.date_pattern = sargs.get_one::<String>("date-pattern").cloned();
            conf.text.nonstandard_measurement_pattern =
                sargs.get_one::<String>("ns-meas-pattern").cloned();

            conf.text.ensure_time = sargs.get_flag("ensure-time");
            conf.text.ensure_time_linear = sargs.get_flag("ensure-time-linear");
            conf.text.ensure_time_nogain = sargs.get_flag("ensure-time-nogain");
            conf.text.disallow_deviant = sargs.get_flag("disallow-deviant");
            conf.text.disallow_nonstandard = sargs.get_flag("disallow-nonstandard");
            conf.text.disallow_deprecated = sargs.get_flag("disallow-deprecated");
            conf.text.raw.repair_offset_spaces = sargs.get_flag("repair-offset-spaces");

            match api::read_fcs_text(filepath, &conf).unwrap() {
                Err(err) => err.print(),
                Ok(res) => {
                    if sargs.get_flag("header") {
                        print_json(&res.header);
                    }
                    if sargs.get_flag("raw") {
                        print_json(&res.raw);
                    }
                    print_json(&res.standard);
                    print_json(&res.nonfatal);
                }
            }
        }

        Some(("data", sargs)) => {
            get_text_delta(sargs);
            // TODO add DATA delta adjust
            conf.text.raw.repair_offset_spaces = sargs.get_flag("repair-offset-spaces");
            let delim = sargs.get_one::<String>("delimiter").unwrap();

            match api::read_fcs_file(filepath, &conf).unwrap() {
                Err(err) => err.print(),
                Ok(res) => api::print_parsed_data(&res, delim),
            }
        }

        _ => (),
    }
}

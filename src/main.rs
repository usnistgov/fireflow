// TODO gating parameters not added (yet)

mod api;
mod numeric;

use clap::{Args, Parser, Subcommand};
use serde::ser::Serialize;
use std::path;

#[derive(Parser)]
struct CLIConfig {
    #[command(subcommand)]
    command: Command,
    filepath: path::PathBuf,
}

#[derive(Subcommand)]
enum Command {
    Json(CLIJson),
    DumpData(CLIDumpData),
    DumpMeas(CLIDumpData),
    DumpSpill(CLIDumpData),
}

#[derive(Args)]
struct CLIJson {
    #[arg(short = 'H', long)]
    header: bool,

    #[arg(short = 'r', long)]
    raw: bool,

    #[arg(short = 'M', long)]
    metadata: bool,

    #[arg(short = 'm', long)]
    measurements: bool,

    #[arg(short = 'd', long)]
    data: bool,

    #[arg(short = 't', long)]
    time_shortname: Option<String>,

    #[arg(short = 'T', long)]
    ensure_time: bool,

    #[arg(short = 's', long)]
    ensure_time_timestep: bool,
}

#[derive(Args)]
struct CLIDumpData {
    #[arg(short = 'd', long, default_value_t = String::from("\t"))]
    delim: String,
}

fn print_json<T: Serialize>(j: &T) {
    println!("{}", serde_json::to_string(j).unwrap());
}

fn main() {
    let args = CLIConfig::parse();

    let conf = api::Reader::default();
    match args.command {
        Command::DumpData(s) => match api::read_fcs_file(&args.filepath, &conf).unwrap() {
            Err(err) => err.print(),
            Ok(res) => api::print_parsed_data(&res, s.delim.as_str()),
        },
        Command::DumpMeas(s) => match api::read_fcs_text(&args.filepath, &conf).unwrap() {
            Err(err) => err.print(),
            Ok(res) => res.standard.print_meas_table(s.delim.as_str()),
        },
        Command::DumpSpill(s) => match api::read_fcs_text(&args.filepath, &conf).unwrap() {
            Err(err) => err.print(),
            Ok(res) => res.standard.print_spillover_table(s.delim.as_str()),
        },
        Command::Json(s) => {
            let conf = api::Reader {
                text: api::StdTextReader {
                    time_shortname: s.time_shortname,
                    ensure_time: s.ensure_time,
                    ensure_time_timestep: s.ensure_time_timestep,
                    ..conf.text
                },
                ..conf
            };
            if s.metadata || s.measurements {
                match api::read_fcs_text(&args.filepath, &conf).unwrap() {
                    Err(err) => err.print(),
                    Ok(res) => {
                        if s.header {
                            print_json(&res.header);
                        }
                        if s.raw {
                            print_json(&res.raw);
                        }
                        print_json(&res.standard);
                    }
                }
            } else if s.raw {
                let (header, raw) = api::read_fcs_raw_text(&args.filepath, &conf).unwrap();
                if s.header {
                    print_json(&header);
                }
                print_json(&raw);
            } else if s.header {
                let header = api::read_fcs_header(&args.filepath).unwrap();
                print_json(&header);
            }
        }
    }

    // let reader = api::std_reader();
    // let res = api::read_fcs_file(file, reader).unwrap().unwrap();
    // println!("{:#?}", res.std);
    // let mut reader = BufReader::new(file);
    // let header = read_header(&mut reader).unwrap();
    // if conf.show_header {
    //     println!("{:#?}", header.clone());
    // }
    // let text = read_raw_text(&mut reader, &header).unwrap();
    // let stext = AnyTEXT::from_kws(header, text);
    // if let Ok(x) = stext {
    //     if conf.show_text {
    //         println!("{:#?}", x.text);
    //     }
    //     for w in x.warnings {
    //         println!("{:#?}", w);
    //     }
    //     if conf.show_data {
    //         let df = read_data(&mut reader, x.data_parser).unwrap();
    //         print_data(x.text, df);
    //     }
    // }
}

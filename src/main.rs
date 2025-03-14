// TODO gating parameters not added (yet)

mod api;
mod numeric;

use clap::{Args, Parser, Subcommand};
use std::path;

// fn print_data(text: AnyTEXT, data: ParsedData) {
//     let shortnames = match &text {
//         AnyTEXT::TEXT2_0(x) => x.standard.get_shortnames(),
//         AnyTEXT::TEXT3_0(x) => x.standard.get_shortnames(),
//         AnyTEXT::TEXT3_1(x) => x.standard.get_shortnames(),
//         AnyTEXT::TEXT3_2(x) => x.standard.get_shortnames(),
//     };
//     let nrows = data[0].len();
//     let ncols = data.len();
//     for s in shortnames {
//         print!("{}", s);
//         print!("\t");
//     }
//     println!();
//     for r in 0..nrows {
//         for c in 0..ncols {
//             print!("{}", data[c].format(r));
//             print!("\t");
//         }
//         println!();
//     }
// }

#[derive(Parser)]
struct CLIConfig {
    #[command(subcommand)]
    command: Command,
    filepath: path::PathBuf,
}

#[derive(Subcommand)]
enum Command {
    JSON(CLIShow),
    // DumpData,
    // DumpSpill,
    // DumpMeasurements,
}

#[derive(Args)]
struct CLIShow {
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
}

fn main() {
    let args = CLIConfig::parse();

    let conf = api::Reader::default();
    match args.command {
        Command::JSON(s) => {
            if s.metadata || s.measurements {
                let res = api::read_fcs_file(args.filepath, conf).unwrap().unwrap();
                if s.header {
                    println!("{}", serde_json::to_string(&res.header).unwrap());
                }
                if s.raw {
                    println!("{}", serde_json::to_string(&res.raw).unwrap());
                }
                println!("{}", serde_json::to_string(&res.std).unwrap());
            } else if s.raw {
                let (header, raw) = api::read_fcs_raw_text(&args.filepath, &conf).unwrap();
                if s.header {
                    header.print();
                }
                raw.print();
            } else if s.header {
                api::read_fcs_header(&args.filepath).unwrap().print();
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

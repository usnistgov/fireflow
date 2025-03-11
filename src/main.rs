// TODO gating parameters not added (yet)

mod api;
mod numeric;

// use clap::{value_parser, Parser};
// use std::fs;
use std::env;
use std::path;

// #[derive(Parser)]
// struct CLIConfig {
//     filepath: path::PathBuf,
//     #[arg(short = 'H', long)]
//     show_header: bool,
//     #[arg(short = 't', long)]
//     show_text: bool,
//     #[arg(short = 'd', long)]
//     show_data: bool,
// }

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

fn main() {
    // let conf = CLIConfig::parse();
    let args: Vec<String> = env::args().collect();

    let args = &args[1..];

    let file = path::PathBuf::from(&args[1]);
    let reader = api::std_reader();
    api::read_fcs_file(file, reader).unwrap().unwrap();
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

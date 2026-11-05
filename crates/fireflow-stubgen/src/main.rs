fn main() {
    let stubs = include_str!(concat!(env!("OUT_DIR"), "/_pyreflow.pyi"));
    print!("{stubs}");
}

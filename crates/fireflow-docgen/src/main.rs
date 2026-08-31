fn main() {
    let common_issues = include_str!(concat!(env!("OUT_DIR"), "/COMMON_ISSUES.md"));
    print!("{common_issues}");
}

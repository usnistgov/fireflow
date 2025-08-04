/// Assert that Display and FromStr are perfect inverses for given input
pub(crate) fn assert_from_to_str<T>(s: &str)
where
    T: std::str::FromStr + std::fmt::Display,
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    match s.parse::<T>() {
        Ok(x) => {
            let ss = x.to_string();
            assert_eq!(s, ss.as_str());
        }
        Err(e) => panic!("could not parse {s}, got error: {e}"),
    }
}

/// Assert that Display and FromStr are near-perfect inverses for given input
pub(crate) fn assert_from_to_str_almost<T>(s0: &str, s1: &str)
where
    T: std::str::FromStr + std::fmt::Display,
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    match s0.parse::<T>() {
        Ok(x) => {
            let ss = x.to_string();
            assert_eq!(s1, ss.as_str());
        }
        Err(e) => panic!("could not parse {s0}, got error: {e}"),
    }
}

use fireflow_types::nonempty_string::DisplayableNE;

use crate::text::lookup::FromStrWith;

use std::fmt::Display;
use std::str::FromStr;

/// Assert that ToDisplayNE and FromStr are perfect inverses for given input
pub fn assert_from_to_str<T>(s: &str)
where
    T: FromStr + for<'a> DisplayableNE<'a>,
    <T as FromStr>::Err: Display,
{
    match s.parse::<T>() {
        Ok(x) => {
            let ss = x.as_string();
            assert_eq!(s, ss.as_str());
        }
        Err(e) => panic!("could not parse {s}, got error: {e}"),
    }
}

/// Assert that ToDisplayNE and FromStrWith are perfect inverses for given input
pub fn assert_from_to_str_with<T>(
    s: &str,
    payload: <T as FromStrWith>::Payload<'_>,
    conf: &<T as FromStrWith>::Config,
) where
    T: FromStrWith + for<'a> DisplayableNE<'a>,
    <T as FromStrWith>::Err: Display,
{
    match T::from_str_with(s, payload, conf) {
        Ok(x) => {
            let ss = x.native.as_string();
            assert_eq!(s, ss.as_str());
        }
        Err(e) => panic!("could not parse {s}, got error: {e}"),
    }
}

/// Assert that ToDisplayNE and FromStr are near-perfect inverses for given input
pub fn assert_from_to_str_almost<T>(s0: &str, s1: &str)
where
    T: FromStr + for<'a> DisplayableNE<'a>,
    <T as FromStr>::Err: Display,
{
    match s0.parse::<T>() {
        Ok(x) => {
            let ss = x.as_string();
            assert_eq!(s1, ss.as_str());
        }
        Err(e) => panic!("could not parse {s0}, got error: {e}"),
    }
}

/// Assert that ToDisplayNE and FromStr are near-perfect inverses for given input
pub fn assert_from_to_str_almost_with<T>(
    s0: &str,
    s1: &str,
    payload: <T as FromStrWith>::Payload<'_>,
    conf: &<T as FromStrWith>::Config,
) where
    T: FromStrWith + for<'a> DisplayableNE<'a>,
    <T as FromStrWith>::Err: Display,
{
    match T::from_str_with(s0, payload, conf) {
        Ok(x) => {
            let ss = x.native.as_string();
            assert_eq!(s1, ss.as_str());
        }
        Err(e) => panic!("could not parse {s0}, got error: {e}"),
    }
}

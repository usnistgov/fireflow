use crate::text::lookup::FromStrWith;
use crate::text::optional::DisplayMaybe;

use std::fmt::Display;
use std::str::FromStr;

/// Assert that Display and FromStr are perfect inverses for given input
pub fn assert_from_to_str<T>(s: &str)
where
    T: FromStr + Display,
    <T as FromStr>::Err: Display,
{
    match s.parse::<T>() {
        Ok(x) => {
            let ss = x.to_string();
            assert_eq!(s, ss.as_str());
        }
        Err(e) => panic!("could not parse {s}, got error: {e}"),
    }
}

/// Assert that Display and FromStrWith are perfect inverses for given input
pub fn assert_from_to_str_with<T>(
    s: &str,
    payload: <T as FromStrWith>::Payload<'_>,
    conf: &<T as FromStrWith>::Config,
) where
    T: FromStrWith + Display,
    <T as FromStrWith>::Err: Display,
{
    match T::from_str_with(s, payload, conf) {
        Ok(x) => {
            let ss = x.native.to_string();
            assert_eq!(s, ss.as_str());
        }
        Err(e) => panic!("could not parse {s}, got error: {e}"),
    }
}

/// Assert that Display and FromStr are perfect inverses for given input
pub fn assert_from_to_str_maybe<T>(s: &str)
where
    T: FromStr + DisplayMaybe,
    <T as FromStr>::Err: Display,
{
    match s.parse::<T>() {
        Ok(x) => {
            let ss = x.display_maybe();
            assert_eq!(Some(s.to_owned()), ss);
        }
        Err(e) => panic!("could not parse {s}, got error: {e}"),
    }
}

/// Assert that Display and FromStrWith are perfect inverses for given input
pub fn assert_from_to_str_maybe_with<T>(
    s: &str,
    payload: <T as FromStrWith>::Payload<'_>,
    conf: &<T as FromStrWith>::Config,
) where
    T: FromStrWith + DisplayMaybe,
    <T as FromStrWith>::Err: Display,
{
    match T::from_str_with(s, payload, conf) {
        Ok(x) => {
            let ss = x.native.display_maybe();
            assert_eq!(Some(s.to_owned()), ss);
        }
        Err(e) => panic!("could not parse {s}, got error: {e}"),
    }
}

/// Assert that Display and FromStr are near-perfect inverses for given input
pub fn assert_from_to_str_almost<T>(s0: &str, s1: &str)
where
    T: FromStr + Display,
    <T as FromStr>::Err: Display,
{
    match s0.parse::<T>() {
        Ok(x) => {
            let ss = x.to_string();
            assert_eq!(s1, ss.as_str());
        }
        Err(e) => panic!("could not parse {s0}, got error: {e}"),
    }
}

/// Assert that Display and FromStr are near-perfect inverses for given input
pub fn assert_from_to_str_almost_with<T>(
    s0: &str,
    s1: &str,
    payload: <T as FromStrWith>::Payload<'_>,
    conf: &<T as FromStrWith>::Config,
) where
    T: FromStrWith + Display,
    <T as FromStrWith>::Err: Display,
{
    match T::from_str_with(s0, payload, conf) {
        Ok(x) => {
            let ss = x.native.to_string();
            assert_eq!(s1, ss.as_str());
        }
        Err(e) => panic!("could not parse {s0}, got error: {e}"),
    }
}

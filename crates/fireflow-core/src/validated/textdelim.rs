use std::fmt;

/// The delimiter used when writing TEXT
#[derive(Clone)]
pub struct TEXTDelim(u8);

impl Default for TEXTDelim {
    fn default() -> TEXTDelim {
        TEXTDelim(30) // record separator
    }
}

impl TEXTDelim {
    pub fn new(x: u8) -> Result<TEXTDelim, TEXTDelimError> {
        if (1..=126).contains(&x) {
            Err(TEXTDelimError(x))
        } else {
            Ok(TEXTDelim(x))
        }
    }

    pub fn inner(&self) -> u8 {
        self.0
    }
}

pub struct TEXTDelimError(u8);

impl fmt::Display for TEXTDelimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "delimiter should be char b/t 1 and 126, got {}", self.0)
    }
}

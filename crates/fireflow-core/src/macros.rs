// TODO maybe just use derive_more?

macro_rules! newtype_from {
    ($outer:ident, $inner:ident) => {
        impl From<$inner> for $outer {
            fn from(value: $inner) -> Self {
                $outer(value)
            }
        }
    };
}

pub(crate) use newtype_from;

macro_rules! newtype_disp {
    ($outer:ident) => {
        impl fmt::Display for $outer {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
                write!(f, "{}", self.0)
            }
        }
    };
}

pub(crate) use newtype_disp;

macro_rules! newtype_asref {
    ($from:ident, $to:ident) => {
        impl AsRef<$to> for $from {
            fn as_ref(&self) -> &$to {
                self.0.as_ref()
            }
        }
    };
}

pub(crate) use newtype_asref;

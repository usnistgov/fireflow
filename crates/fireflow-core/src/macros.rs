macro_rules! match_many_to_one {
    ($value:expr, $root:ident, [$($variant:ident),*], $inner:ident, $action:block) => {
        match $value {
            $(
                $root::$variant($inner) => {
                    $action
                },
            )*
        }
    };
}

pub(crate) use match_many_to_one;

macro_rules! impl_newtype_try_from {
    ($outer:ident, $inter:ident, $inner:ident, $err:ident) => {
        impl TryFrom<$inner> for $outer {
            type Error = $err;
            fn try_from(value: $inner) -> Result<Self, Self::Error> {
                $inter::try_from(value).map($outer)
            }
        }
    };
}

pub(crate) use impl_newtype_try_from;

macro_rules! def_failure {
    ($failname:ident, $msg:expr) => {
        // make these pub no matter what since they will be in public error
        // interfaces
        #[derive(Default)]
        pub struct $failname;

        impl fmt::Display for $failname {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
                write!(f, $msg)
            }
        }
    };
}

pub(crate) use def_failure;

macro_rules! match_many_to_one {
    ($value:expr, $root:ident, [$($variant:ident),*], $inner:ident, $action:expr) => {
        match $value {
            $(
                $root::$variant($inner) => $action,
            )*
        }
    };

    ($value:expr, $root:ident, [$($variant:ident),*], mut $inner:ident, $action:block) => {
        match $value {
            $(
                $root::$variant(mut $inner) => {
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

macro_rules! def_summary {
    ($(#[$meta:meta])* $vis:vis $failname:ident, $msg:expr) => {
        $(#[$meta])*
        #[derive(Default, Debug, Clone, Copy, derive_more::Display)]
        #[display($msg)]
        $vis struct $failname;
    };
}

pub(crate) use def_summary;

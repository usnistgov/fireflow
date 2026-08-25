/// Implement a enum with variants that map to defined string literals.
///
/// This will make 4 things:
/// 1. the enum itself (with docs as given)
/// 2. a FromStr impl that maps each variant to a string literal
/// 3. an error for FromStr that lists each string variant
/// 4. an array that contains all string literals in the order given
#[macro_export]
macro_rules! impl_str_enum {
    (@count) => { 0_usize };

    (@count $head:expr $(, $tail:expr)*) => {
        1_usize + $crate::impl_str_enum!(@count $($tail),*)
    };

    ($(#[$flag_meta:meta])* $flag_vis:vis $flag_name:ident,
     $(#[$error_meta:meta])* $error_vis:vis $error_name:ident,
     $($(#[$var_meta:meta])* $var:ident => $strlit:expr),+
    ) => {
        $(#[$flag_meta])*
        #[derive(Clone, Copy)]
        $flag_vis enum $flag_name {
            $(
                $(#[$var_meta])*
                $var,
            )*
        }

        impl std::str::FromStr for $flag_name {
            type Err = $error_name;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                $(
                    if $strlit.as_ref() == s {
                        return Ok(Self::$var);
                    }
                )*
                    Err($error_name(s.to_owned()))
            }
        }

        impl $crate::config::EnumStrIter<{ $crate::impl_str_enum!(@count $($var),*) }> for $flag_name {
            const ITEMS: [Self; { $crate::impl_str_enum!(@count $($var),*) }] = [$(Self::$var),*];

            fn as_ne_str(&self) -> &'static $crate::nonempty_string::NEStr {
                match self {
                    $(Self::$var => $strlit,)*
                }
            }
        }

        $(#[$error_meta])*
        #[derive(thiserror::Error, Debug, PartialEq, Eq, Clone)]
        $error_vis struct $error_name($error_vis String);

        impl std::fmt::Display for $error_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
                // TODO what if this string is really really long?
                let original = &self.0;
                let all: Vec<_> = <$flag_name as $crate::config::EnumStrIter<_>>::iter_str().collect();
                let ne = nonempty_collections::NESlice::try_from_slice(&all[..])
                    .expect("macro should require at least one flag so this should never fail");
                let (last, rest) = $crate::nonempty_string::NESliceExt::split_last(&ne);
                if rest.is_empty() {
                    write!(f, "must be '{last}', got '{original}'")
                } else {
                    write!(f, "must be one of ")?;
                    for r in rest {
                        write!(f, "'{r}', ")?;
                    }
                    write!(f, "or '{last}', got '{original}'")
                }
            }
        }
    };
}

pub use impl_str_enum;

/// Make enum string enum literal to be used as a keyword value.
///
/// This will impl the enum literal and add a ToDisplayNE trait.
#[macro_export]
macro_rules! impl_str_enum_kw {
    ($(#[$flag_meta:meta])* $flag_vis:vis $flag_name:ident,
     $(#[$error_meta:meta])* $error_vis:vis $error_name:ident,
     $($(#[$var_meta:meta])* $var:ident => $strlit:expr),+
    ) => {
        $crate::impl_str_enum!(
            $(#[$flag_meta])* $flag_vis $flag_name,
            $(#[$error_meta])* $error_vis $error_name,
            $($(#[$var_meta])* $var => $strlit),*
        );

        impl $crate::nonempty_string::ToDisplayNE<'_> for $flag_name {
            type NE = &'static $crate::nonempty_string::NEStr;
            fn to_ne(&self) -> Self::NE {
                $crate::config::EnumStrIter::as_ne_str(self)
            }
        }
    };
}

pub use impl_str_enum_kw;

/// Make an enum string literal to be used as a configuration flag.
///
/// In addition to that described in [`impl_str_enum`], this will add:
/// * Default trait for first variant
/// * Display trait for enum
/// * Python to/from traits for both enum and parse error
#[macro_export]
macro_rules! impl_config_flag {
    ($(#[$flag_meta:meta])* $flag_vis:vis $flag_name:ident,
     $(#[$error_meta:meta])* $error_vis:vis $error_name:ident,
     $(#[$var_meta0:meta])* $var0:ident => $strlit0:expr,
     $($(#[$var_meta:meta])* $var:ident => $strlit:expr),*
    ) => {
        $crate::impl_str_enum!(
            #[derive(Display, Default)]
            #[display("{}", self.as_str())]
            #[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
            $(#[$flag_meta])* $flag_vis $flag_name,

            #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
            #[cfg_attr(feature = "python", pyerr($crate::python::ConfigError))]
            $(#[$error_meta])* $error_vis $error_name,

            #[default]
            $(#[$var_meta0])* $var0 => $strlit0,

            $($(#[$var_meta])* $var => $strlit),*
        );
    }
}

pub use impl_config_flag;

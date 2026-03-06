use crate::config::EnumStrIter as _;
use crate::{impl_str_enum, ne_str};

use const_format::formatcp;
use derive_more::Display;
use nonempty_collections::{NEVec, NonEmptyArrayExt as _, nev};
use unicase::Ascii;

#[cfg(feature = "python")]
use {
    crate::python as py,
    fireflow_core_proc::{DisplayAsPyErr, FromPyString, IntoPyString},
};

#[cfg(feature = "serde")]
use serde::Serialize;

// The string primitives for almost all keywords are compiled in a build script
// as string constants and included here. This is done in order to put these
// strings into a pre-compiled hash table which will be used for version
// autodetection and sorting through unused keywords efficiently.
//
// This will also instantiate constants for all keywords and their components:
// - root keyword base strings will be like <NAME>_KW
// - root keywords (with $) will be named just like the base string
// - meas suffixes will be like <NAME>_KW_SUFFIX
// - meas keywords with $ and "n" will be like PN<SUFFIX>
include!(concat!(env!("OUT_DIR"), "/kw_map.rs"));

// other keywords not in build script
pub const PKN: &str = "$PKn";
pub const PKNN: &str = "$PKNn";
pub const RNI: &str = "$RNI";
pub const RNW: &str = "$RNW";

impl_str_enum!(
    /// All FCS versions this library supports.
    ///
    /// This appears as the first 6 bytes of any valid FCS file.
    #[derive(Eq, PartialEq, PartialOrd, Ord, Debug, Hash, Display)]
    #[display("{}", self.as_str())]
    #[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
    #[cfg_attr(feature = "serde", derive(Serialize))]
    pub Version,
    /// Error when parsing [`TriFlag`] from [`String`]
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
    pub VersionFormatError,
    FCS2_0 => ne_str!("FCS2.0"),
    FCS3_0 => ne_str!("FCS3.0"),
    FCS3_1 => ne_str!("FCS3.1"),
    FCS3_2 => ne_str!("FCS3.2")
);

// TODO this could be put in the macro above
pub const ALL_VERSIONS: [Version; 4] = [
    Version::FCS2_0,
    Version::FCS3_0,
    Version::FCS3_1,
    Version::FCS3_2,
];

// marker traits that denote a single version
macro_rules! impl_version {
    ($name:ident, $var:ident) => {
        #[derive(Clone, Copy, Eq, PartialEq)]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        pub struct $name;

        impl From<$name> for Version {
            fn from(_: $name) -> Self {
                Self::$var
            }
        }
    };
}

impl_version!(Version2_0, FCS2_0);
impl_version!(Version3_0, FCS3_0);
impl_version!(Version3_1, FCS3_1);
impl_version!(Version3_2, FCS3_2);

impl Version {
    #[must_use]
    pub fn is_member(self, membership: VersionMembership) -> bool {
        match self {
            Self::FCS2_0 => membership.is_2_0(),
            Self::FCS3_0 => membership.is_3_0(),
            Self::FCS3_1 => membership.is_3_1(),
            Self::FCS3_2 => membership.is_3_2(),
        }
    }
}

/// Data structure to classify root (non-indexed) keywords.
///
/// For optional keywords this simply records the version in which a given
/// keyword is valid. Some specific keywords ($CYT, $TOT, etc) are explicitly
/// encoded since they are optional or required (or missing entirely) depending
/// on version. $BYTEORD is included because a non-endian value implies 2.0/3.0.
/// $MODE is included because its value and optionality is different between 3.1
/// and 3.2
#[derive(Clone, Copy)]
pub enum RootKeywordClass {
    OptAny,
    OptGE3_1,
    OptGE3_2,
    OptEQ3_0or3_1,
    OptEQ3_0,
    OptLE3_1,
    Mode,
    Cyt,
    Tot,
    Timestep,
    Byteord,
    Begindata,
    Enddata,
    Beginanalysis,
    Endanalysis,
    Beginstext,
    Endstext,
}

impl RootKeywordClass {
    #[must_use]
    pub const fn membership(&self) -> VersionMembership {
        match self {
            Self::OptAny | Self::Mode | Self::Cyt | Self::Tot | Self::Byteord => {
                VersionMembership::All
            }
            Self::OptGE3_1 => VersionMembership::Two([Version::FCS3_1, Version::FCS3_2]),
            Self::OptGE3_2 => VersionMembership::One(Version::FCS3_2),
            Self::OptEQ3_0or3_1 => VersionMembership::Two([Version::FCS3_0, Version::FCS3_1]),
            Self::OptEQ3_0 => VersionMembership::One(Version::FCS3_0),
            Self::OptLE3_1 => {
                VersionMembership::Three([Version::FCS2_0, Version::FCS3_0, Version::FCS3_1])
            }
            Self::Timestep
            | Self::Begindata
            | Self::Enddata
            | Self::Beginanalysis
            | Self::Endanalysis
            | Self::Beginstext
            | Self::Endstext => {
                VersionMembership::Three([Version::FCS3_0, Version::FCS3_1, Version::FCS3_2])
            }
        }
    }
}

#[derive(Clone, Copy)]
pub enum MeasKeywordClass {
    OptAny,
    OptGE3_0,
    OptGE3_1,
    OptGE3_2,
    Scale,
    Shortname,
    Wavelength,
}

impl MeasKeywordClass {
    #[must_use]
    pub const fn membership(&self) -> VersionMembership {
        match self {
            Self::OptAny | Self::Scale | Self::Shortname | Self::Wavelength => {
                VersionMembership::All
            }
            Self::OptGE3_0 => {
                VersionMembership::Three([Version::FCS3_0, Version::FCS3_1, Version::FCS3_2])
            }
            Self::OptGE3_1 => VersionMembership::Two([Version::FCS3_1, Version::FCS3_2]),
            Self::OptGE3_2 => VersionMembership::One(Version::FCS3_2),
        }
    }
}

#[derive(Clone, Copy)]
pub enum VersionMembership {
    One(Version),
    Two([Version; 2]),
    Three([Version; 3]),
    All,
}

impl VersionMembership {
    #[must_use]
    pub fn versions(self) -> NEVec<Version> {
        match self {
            Self::One(x) => NEVec::new(x),
            Self::Two([x, y]) => nev![x, y],
            Self::Three([x, y, z]) => nev![x, y, z],
            Self::All => ALL_VERSIONS.into_nonempty_vec(),
        }
    }

    #[must_use]
    pub fn is_2_0(&self) -> bool {
        self.contains_version(Version::FCS2_0)
    }

    #[must_use]
    pub fn is_3_0(&self) -> bool {
        self.contains_version(Version::FCS3_0)
    }

    #[must_use]
    pub fn is_3_1(&self) -> bool {
        self.contains_version(Version::FCS3_1)
    }

    #[must_use]
    pub fn is_3_2(&self) -> bool {
        self.contains_version(Version::FCS3_2)
    }

    #[must_use]
    pub fn contains_version(self, version: Version) -> bool {
        match self {
            Self::One(x) => x == version,
            Self::Two(xs) => xs.contains(&version),
            Self::Three(xs) => xs.contains(&version),
            Self::All => true,
        }
    }
}

// BYTEORD big/little flags

pub const BYTEORD_BIG: &str = "big";
pub const BYTEORD_LITTLE: &str = "little";

// Scale Diagnostic flags

pub const SCALE_DIAGNOSTIC_FORCED: &str = "forced";
pub const SCALE_DIAGNOSTIC_LOG: &str = "log";
pub const SCALE_DIAGNOSTIC_TRIMMED: &str = "trimmed";
pub const SCALE_DIAGNOSTIC_TRIMMED_LOG: &str = "trimmed_log";

pub const TEMPORAL_SCALE_DIAGNOSTIC_FORCED: &str = "forced";
pub const TEMPORAL_SCALE_DIAGNOSTIC_TRIMMED: &str = "trimmed";

// ISO datetime formats

pub const ISO_DATETIME_NO_TZ: &str = "%Y-%m-%dT%H:%M:%S%.f";
pub const ISO_DATETIME_TZ_HH_MAYBE_MM: &str = formatcp!("{ISO_DATETIME_NO_TZ}%#z");
pub const ISO_DATETIME_TZ_HH_MM: &str = formatcp!("{ISO_DATETIME_NO_TZ}%:z");
pub const ISO_DATETIME_TZ_HH: &str = formatcp!("{ISO_DATETIME_NO_TZ}%:::z");

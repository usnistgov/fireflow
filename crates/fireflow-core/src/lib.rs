#![warn(clippy::shadow_reuse)]
#![warn(clippy::shadow_unrelated)]

pub mod api;
pub mod config;
pub mod core;
pub mod data;
pub mod error;
pub mod header;
mod macros;
pub mod nonempty;
#[cfg(feature = "python")]
pub mod python;
pub mod segment;
#[cfg(test)]
mod test;
pub mod text;
pub mod validated;

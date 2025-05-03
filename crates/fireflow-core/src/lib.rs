#![warn(clippy::shadow_reuse)]
#![warn(clippy::shadow_unrelated)]

pub mod api;
pub mod config;
pub mod core;
pub mod error;
mod header;
mod header_text;
mod keywords;
mod macros;
mod optionalkw;
mod segment;
pub mod validated;

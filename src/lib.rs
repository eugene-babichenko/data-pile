#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

mod appender;
mod error;
mod flatfile;

use appender::Appender;
pub use error::Error;

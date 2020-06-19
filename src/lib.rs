#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

mod error;
mod flatfile;

pub use error::Error;

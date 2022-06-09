//! # `pile` - a simple and fast append-only data store
//!
//! ## Design goals
//!
//! * Efficient adding of bug chunks of data.
//! * A user should be able to copy the storage data (for example, over the network)
//!   while still being able to use the database for both reads and writes.
//! * The storage should have a minimal dependency footprint.
//!
//! ## Usage guide
//!
//! ### Example
//!
//! ```rust,ignore
//! use data_pile::Database;
//! let db = Database::new("./pile").unwrap();
//! let value = b"some data";
//! db.put(&value).unwrap();
//! ```

#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;
extern crate core;

mod appender;
mod database;
mod error;
mod flatfile;
mod growable_mmap;
mod index_on_mmaps;
mod seqno;
mod seqno_iter;
mod shared_mmap;

use appender::Appender;
pub use database::Database;
pub use error::Error;
pub use seqno_iter::SeqNoIter;
pub use shared_mmap::SharedMmap;

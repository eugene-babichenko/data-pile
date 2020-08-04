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
//! ````
//!
//! ### Transferring the data
//!
//! - Get the raw data by using `Database::snapshot()`.
//! - Copy it somewhere.
//! - Create the root direcotry of the data store, create a file named `data` in it
//!   and add all snapshot data into it.
//! - Just start using the database: it will verify correctness and rebuild all
//!   indexes.

#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

mod appender;
mod database;
mod error;
mod flatfile;
mod growable_mmap;
mod page_index;
mod seqno;
mod seqno_iter;
mod shared_mmap;

use appender::Appender;
pub use database::Database;
pub use error::Error;
pub use seqno_iter::SeqNoIter;
pub use shared_mmap::SharedMmap;

use std::{error, fmt, io, path::PathBuf};

/// Datbase error.
#[derive(Debug)]
pub enum Error {
    /// Failed to open file.
    FileOpen(PathBuf, io::Error),
    /// Failed to create mmap.
    Mmap(io::Error),
    /// Database path already exists and does not point to a directory
    PathNotDir,
    /// The record with this key already exists.
    RecordExists(usize),
    /// Records in the data file are incorrect.
    DataFileDamaged,
    /// Sequential number index is broken
    SeqNoIndexDamaged,
    /// Failed to extend a file
    Extend(io::Error),
    /// Failed to flush database records to disk
    Flush(io::Error),
    /// Failed to get file metadata
    Metadata(io::Error),
    /// Failed to make a memory mapping page immutable
    Protect(io::Error),
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::FileOpen(_, source) => Some(source),
            Error::Mmap(source) => Some(source),
            Error::PathNotDir => None,
            Error::RecordExists(_) => None,
            Error::DataFileDamaged => None,
            Error::SeqNoIndexDamaged => None,
            Error::Extend(source) => Some(source),
            Error::Flush(source) => Some(source),
            Error::Metadata(source) => Some(source),
            Error::Protect(source) => Some(source),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::FileOpen(path, _) => write!(f, "failed to open file `{}`", path.display()),
            Error::Mmap(_) => write!(f, "memory map failed"),
            Error::PathNotDir => write!(
                f,
                "database path already exists and does not point to a directory"
            ),
            Error::RecordExists(id) => write!(f, "a record with id {} already exists", id),
            Error::DataFileDamaged => write!(f, "data file looks damaged"),
            Error::SeqNoIndexDamaged => write!(f, "sequential number index file looks damaged"),
            Error::Extend(_) => write!(f, "failed to extend a database file"),
            Error::Flush(_) => write!(f, "failed to flush database records to disk"),
            Error::Metadata(_) => write!(f, "failed to get file metadata"),
            Error::Protect(_) => write!(f, "failed to make a memory mapping page immutable"),
        }
    }
}

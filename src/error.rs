use std::{error, fmt, fmt::Write, io, path::PathBuf};

#[derive(Debug)]
pub enum Error {
    /// Failed to open file.
    FileOpen(PathBuf, io::Error),
    /// Failed to create mmap.
    Mmap(io::Error),
    // mmap is too small for a file to be extended.
    MmapTooSmall,
    /// Error while extending a file.
    Write(io::Error),
    /// Database path already exists and does not point to a directory
    PathNotDir,
    /// The record with this key already exists.
    RecordExists(Vec<u8>),
    /// Records in the data file are incorrect.
    DataFileDamaged,
    /// Sequential number index is broken
    SeqNoIndexDamaged,
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::FileOpen(_, source) => Some(source),
            Error::Mmap(source) => Some(source),
            Error::MmapTooSmall => None,
            Error::Write(source) => Some(source),
            Error::PathNotDir => None,
            Error::RecordExists(_) => None,
            Error::DataFileDamaged => None,
            Error::SeqNoIndexDamaged => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::FileOpen(path, _) => write!(f, "failed to open file `{}`", path.display()),
            Error::Mmap(_) => write!(f, "memory map failed"),
            Error::MmapTooSmall => write!(f, "the map size is too little to write new records"),
            Error::Write(_) => write!(f, "failed to write the file"),
            Error::PathNotDir => write!(
                f,
                "database path already exists and does not point to a directory"
            ),
            Error::RecordExists(id) => write!(f, "a record with id {} already exists", hex(&id)),
            Error::DataFileDamaged => write!(f, "data file looks damaged"),
            Error::SeqNoIndexDamaged => write!(f, "sequential number index file looks damaged"),
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut s, "{:2x}", byte).unwrap();
    }
    s
}

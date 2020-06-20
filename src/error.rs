use std::{error, fmt, io, path::PathBuf};

#[derive(Debug)]
pub enum Error {
    /// Failed to open file.
    FileOpen(PathBuf, io::Error),
    /// Failed to create mmap.
    Mmap(PathBuf, io::Error),
    // mmap is too small for a file to be extended.
    MmapTooSmall,
    /// Error while extending a file.
    Write(io::Error),
    /// Database path already exists and does not point to a directory
    PathNotDir,
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::FileOpen(_, source) => Some(source),
            Error::Mmap(_, source) => Some(source),
            Error::MmapTooSmall => None,
            Error::Write(source) => Some(source),
            Error::PathNotDir => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::FileOpen(path, _) => write!(f, "failed to open file `{}`", path.display()),
            Error::Mmap(path, _) => write!(f, "memory map failed for file `{}`", path.display()),
            Error::MmapTooSmall => write!(f, "the map size is too little to write new records"),
            Error::Write(_) => write!(f, "failed to write the file"),
            Error::PathNotDir => write!(
                f,
                "database path already exists and does not point to a directory"
            ),
        }
    }
}

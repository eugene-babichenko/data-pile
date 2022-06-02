use crate::{Appender, Error};
use std::{io::Write, path::PathBuf};

/// Flatfiles are the main database files that hold all keys and data.
///
/// Records are stored without any additional spaces. The file does not hold any
/// additional data.
///
/// A flatfile is opened with `mmap` and we rely on OS's mechanisms for caching
/// pages, etc.
pub(crate) struct FlatFile {
    inner: Appender,
}

/// Low-level interface to flatfiles.
impl FlatFile {
    /// Open a flatfile.
    ///
    /// # Arguments
    ///
    /// * `path` - the path to the file. It will be created if not exists.
    pub fn new(path: Option<PathBuf>, writable: bool) -> Result<Self, Error> {
        Appender::new(path, writable).map(|inner| FlatFile { inner })
    }

    /// Write an array of records to the drive. This function will block if
    /// another write is still in progress.
    pub fn append<'a>(&'a self, records: &[&[u8]]) -> Result<(), Error> {
        if records.is_empty() {
            return Ok(());
        }

        let size_inc: usize = records
            .iter()
            .map(|record| {
                assert!(!record.is_empty(), "empty records are not supported");
                record.len()
            })
            .sum();

        self.inner.append(size_inc, move |mut mmap| {
            for record in records {
                mmap.write_all(record).map_err(Error::MmapWrite)?;
            }
            Ok(())
        })
    }

    /// Get the value at the given `offset`. If the `offset` is outside of the
    /// file boundaries, `None` is returned. Upon a successul read a key-value
    /// record is returned. Note that this function do not check if the given
    /// `offset` is the start of an actual record, so you should be careful when
    /// using it.
    pub fn get_record_at_offset(&self, offset: usize, length: usize) -> Option<Vec<u8>> {
        self.inner.get_data(offset, move |mmap| {
            if mmap.len() < length {
                return None;
            }

            Some(mmap[..length].to_vec())
        })
    }

    pub fn len(&self) -> usize {
        self.inner.size()
    }
}

#[cfg(test)]
mod tests {
    use super::FlatFile;

    #[quickcheck]
    fn test_read_write(records: Vec<Vec<u8>>) {
        if records.is_empty() {
            return;
        }

        let tmp = tempfile::NamedTempFile::new().unwrap();

        let raw_records: Vec<_> = records
            .iter()
            .filter(|x| !x.is_empty())
            .map(|x| x.as_ref())
            .collect();

        let flatfile = FlatFile::new(Some(tmp.path().to_path_buf()), true).unwrap();
        flatfile.append(&raw_records).unwrap();

        let mut offset = 0;
        for record in raw_records.iter() {
            let drive_record = flatfile.get_record_at_offset(offset, record.len()).unwrap();
            assert_eq!(*record, drive_record.as_slice());
            offset += drive_record.len();
        }
    }
}

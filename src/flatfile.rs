use crate::{Appender, Error};
use std::{io::Write, mem::size_of, path::Path};

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
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        Appender::new(path).map(|inner| FlatFile { inner })
    }

    /// Write an array of records to the drive. This function will block if
    /// another write is still in progress.
    pub fn append(&self, records: &[&[u8]]) -> Result<(), Error> {
        let size_inc: usize = records
            .iter()
            .fold(0, |value, record| value + record.len() + size_of::<u64>());

        self.inner.append(size_inc, move |mut mmap| {
            for record in records {
                mmap.write_all(&(record.len() as u64).to_le_bytes()[..])
                    .unwrap();
                mmap.write_all(record).unwrap();
            }
        })
    }

    /// Get the value at the given `offset`. If the `offset` is outside of the
    /// file boundaries, `None` is returned. Upon a successul read a key-value
    /// record is returned. Note that this function do not check if the given
    /// `offset` is the start of an actual record, so you should be careful when
    /// using it.
    pub fn get_record_at_offset(&self, offset: usize) -> Option<&[u8]> {
        self.inner.get_data(offset, move |mut mmap| {
            if mmap.len() < size_of::<u64>() {
                return None;
            }

            let mut value_length_bytes = [0u8; size_of::<u64>()];
            value_length_bytes.copy_from_slice(&mmap[..size_of::<u64>()]);
            let value_length = u64::from_le_bytes(value_length_bytes) as usize;
            mmap = &mmap[size_of::<u64>()..];

            if mmap.len() < value_length {
                return None;
            }

            let value = &mmap[..value_length];

            Some(value)
        })
    }

    pub fn len(&self) -> usize {
        self.inner.size()
    }

    /// Get the pointer to the underlying raw data.
    pub fn snapshot(&self) -> Result<impl AsRef<[u8]>, Error> {
        self.inner.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::FlatFile;
    use std::mem::size_of;

    #[quickcheck]
    fn test_read_write(records: Vec<Vec<u8>>) {
        if records.is_empty() {
            return;
        }

        let tmp = tempfile::NamedTempFile::new().unwrap();

        let raw_records: Vec<_> = records.iter().map(|x| x.as_ref()).collect();

        let flatfile = FlatFile::new(tmp.path()).unwrap();
        flatfile.append(&raw_records).unwrap();

        let mut offset = 0;
        for record in raw_records.iter() {
            let drive_record = flatfile.get_record_at_offset(offset).unwrap();
            assert_eq!(*record, drive_record);
            offset += drive_record.len() + size_of::<u64>();
        }
    }
}

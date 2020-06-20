use crate::{Appender, Error};
use std::{mem::size_of, path::Path};

/// Index from the sequential number of a record to its location in a flatfile.
pub(crate) struct SeqNoIndex {
    inner: Appender,
}

impl SeqNoIndex {
    /// Open an index.
    ///
    /// # Arguments
    ///
    /// * `path` - the path to the file. It will be created if not exists.
    /// * `map_size` - the size of the memory map that will be used. This map
    ///   limits the size of the file. If the `map_size` is smaller than the
    ///   size of the file, an error will be returned.
    pub fn new<P: AsRef<Path>>(path: P, map_size: usize) -> Result<Self, Error> {
        Appender::new(path, map_size).map(|inner| Self { inner })
    }

    /// Add records to index. This function will block if another write is still
    /// in progress.
    pub fn append(&self, records: &[u64]) -> Result<(), Error> {
        let size_inc: usize = records.len() * size_of::<u64>();

        self.inner.append(size_inc, move |mut mmap| {
            for record in records {
                mmap[..size_of::<u64>()].copy_from_slice(&record.to_le_bytes()[..]);
                mmap = &mut mmap[size_of::<u64>()..];
            }
        })
    }

    /// Get the location of a record with the given number.
    pub fn get_pointer_to_value(&self, seqno: usize) -> Option<u64> {
        self.inner.get_data(|mmap| {
            let offset = seqno * size_of::<u64>();

            if mmap.len() < offset + size_of::<u64>() {
                return None;
            }

            let mut key_length_bytes = [0u8; size_of::<u64>()];
            key_length_bytes.copy_from_slice(&mmap[offset..(offset + size_of::<u64>())]);

            Some(u64::from_le_bytes(key_length_bytes))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::SeqNoIndex;
    use std::mem::size_of;

    #[quickcheck]
    fn test_read_write(records: Vec<u64>) {
        if records.is_empty() {
            return;
        }

        let tmp = tempfile::NamedTempFile::new().unwrap();

        let index = SeqNoIndex::new(tmp.path(), records.len() * size_of::<u64>()).unwrap();
        index.append(&records).unwrap();

        for (i, record) in records.iter().enumerate() {
            let drive_record = index.get_pointer_to_value(i).unwrap();
            assert_eq!(*record, drive_record);
        }
    }
}

use crate::{Appender, Error};
use std::{mem::size_of, path::PathBuf};

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
    pub fn new(path: Option<PathBuf>) -> Result<Self, Error> {
        Appender::new(path).map(|inner| Self { inner })
    }

    /// Add records to index. This function will block if another write is still
    /// in progress.
    pub fn append(&self, records: &[u64]) -> Result<Option<usize>, Error> {
        if records.is_empty() {
            return Ok(None);
        }

        let size_inc: usize = records.len() * size_of::<u64>();
        let current_seqno = self.inner.size() / size_of::<u64>();

        self.inner.append(size_inc, move |mut mmap| {
            for record in records {
                mmap[..size_of::<u64>()].copy_from_slice(&record.to_le_bytes()[..]);
                mmap = &mut mmap[size_of::<u64>()..];
            }
        })?;

        Ok(Some(current_seqno))
    }

    /// Get the location of a record with the given number.
    pub fn get_pointer_to_value(&self, seqno: usize) -> Option<u64> {
        let offset = seqno * size_of::<u64>();

        self.inner.get_data(offset, |mmap| {
            let mut key_length_bytes = [0u8; size_of::<u64>()];
            key_length_bytes.copy_from_slice(&mmap.as_ref()[..size_of::<u64>()]);

            Some(u64::from_le_bytes(key_length_bytes))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::SeqNoIndex;

    #[quickcheck]
    fn test_read_write(records: Vec<u64>) {
        if records.is_empty() {
            return;
        }

        let tmp = tempfile::NamedTempFile::new().unwrap();

        let index = SeqNoIndex::new(Some(tmp.path().to_path_buf())).unwrap();
        index.append(&records).unwrap();

        for (i, record) in records.iter().enumerate() {
            let drive_record = index.get_pointer_to_value(i).unwrap();
            assert_eq!(*record, drive_record);
        }
    }

    #[quickcheck]
    fn test_seq_number(records: Vec<u64>) {
        let tmp = tempfile::NamedTempFile::new().unwrap();

        let index = SeqNoIndex::new(Some(tmp.path().to_path_buf())).unwrap();
        let checks_count = 100usize;
        for i in 0..checks_count {
            let result = index.append(&records).unwrap();
            if !records.is_empty() {
                assert_eq!(result.unwrap(), i * records.len());
            } else {
                assert!(result.is_none());
            }
            assert!(index
                .get_pointer_to_value((i + 1) * records.len())
                .is_none());
            if !records.is_empty() {
                assert!(index
                    .get_pointer_to_value(i * records.len() + records.len() - 1)
                    .is_some());
            }
        }
    }
}

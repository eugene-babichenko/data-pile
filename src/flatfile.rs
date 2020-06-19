//! Flatfiles are the main database files that hold all keys and data. These
//! files hold data in the very simple form:
//!
//! * key length - 8 bytes
//! * value length - 8 bytes
//! * key bytes
//! * value bytes
//!
//! Length values are recorded as little-endian. They are located next to each
//! other to make use of CPU caches.
//!
//! These records are recorded without any additional spaces. The file does not
//! hold any additional data.
//!
//! A flatfile is opened with `mmap` and we rely on OS's mechanisms for caching
//! pages, etc.

use crate::{Appender, Error};
use std::{mem::size_of, path::Path};

pub(crate) struct FlatFile {
    inner: Appender,
}

pub(crate) struct RawRecord<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

/// Low-level interface to flatfiles.
impl FlatFile {
    /// Open a flatfile.
    ///
    /// # Arguments
    ///
    /// * `path` - the path to the file. It will be created if not exists.
    /// * `map_size` - the size of the memory map that will be used. This map
    ///   limits the size of the file. If the `map_size` is smaller than the
    ///   size of the file, an error will be returned.
    pub fn new<P: AsRef<Path>>(path: P, map_size: usize) -> Result<Self, Error> {
        Appender::new(path, map_size).map(|inner| FlatFile { inner })
    }

    /// Write an array of records to the drive. This function will block if
    /// another write is still in progress.
    pub fn append(&self, records: &[RawRecord]) -> Result<(), Error> {
        let size_inc: usize = records
            .iter()
            .fold(0, |value, record| value + record.size());

        self.inner.append(size_inc, move |mmap| {
            let mut offset = 0;
            for record in records {
                mmap[offset..(offset + size_of::<u64>())]
                    .copy_from_slice(&(record.key.len() as u64).to_le_bytes()[..]);
                offset += size_of::<u64>();

                mmap[offset..(offset + size_of::<u64>())]
                    .copy_from_slice(&(record.value.len() as u64).to_le_bytes()[..]);
                offset += size_of::<u64>();

                mmap[offset..(offset + record.key.len())].copy_from_slice(&record.key);
                offset += record.key.len();

                mmap[offset..(offset + record.value.len())].copy_from_slice(&record.value);
                offset += record.value.len();
            }
        })
    }

    /// Get the value at the given `offset`. If the `offset` is outside of the
    /// file boundaries, `None` is returned. Upon a successul read a tuple of
    /// a key-value record and the physical size of this record is returned.
    /// Note that this function do not check if the given `offset` is the start
    /// of an actual record, so you should be careful when using it.
    pub fn get_record_at_offset(&self, offset: usize) -> Option<(RawRecord, usize)> {
        self.inner.get_data(move |mmap| {
            let mut offset = offset;

            let actual_size = mmap.len();

            if actual_size < offset + size_of::<u64>() * 2 {
                return None;
            }

            let end = offset + size_of::<u64>();
            let mut key_length_bytes = [0u8; size_of::<u64>()];
            key_length_bytes.copy_from_slice(&mmap[offset..end]);
            let key_length = u64::from_le_bytes(key_length_bytes) as usize;
            offset += size_of::<u64>();

            let end = offset + size_of::<u64>();
            let mut value_length_bytes = [0u8; size_of::<u64>()];
            value_length_bytes.copy_from_slice(&mmap[offset..end]);
            let value_length = u64::from_le_bytes(value_length_bytes) as usize;
            offset += size_of::<u64>();

            if actual_size < offset + key_length + value_length {
                return None;
            }

            let end = offset + key_length;
            let key = &mmap[offset..end];
            offset += key_length;

            let end = offset + value_length;
            let value = &mmap[offset..end];

            let record_size = value_length + key_length + size_of::<u64>() * 2;

            Some((RawRecord { key, value }, record_size))
        })
    }
}

impl<'a> RawRecord<'a> {
    pub fn new(key: &'a [u8], value: &'a [u8]) -> RawRecord<'a> {
        Self { key, value }
    }

    pub fn key(&self) -> &'a [u8] {
        self.key
    }

    pub fn value(&self) -> &'a [u8] {
        self.value
    }

    pub fn size(&self) -> usize {
        self.key.len() + self.value.len() + size_of::<u64>() * 2
    }
}

#[cfg(test)]
mod tests {
    use super::{FlatFile, RawRecord};
    use crate::Error;
    use quickcheck::{Arbitrary, Gen};
    use std::sync::Arc;

    #[derive(Debug, Clone)]
    struct TestRecord {
        key: Vec<u8>,
        value: Vec<u8>,
    }

    impl Arbitrary for TestRecord {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            Self {
                key: Arbitrary::arbitrary(g),
                value: Arbitrary::arbitrary(g),
            }
        }
    }

    fn convert_records(records: &[TestRecord]) -> (Vec<RawRecord>, usize) {
        let raw_records: Vec<_> = records
            .iter()
            .map(|record| RawRecord::new(&record.key, &record.value))
            .collect();

        let map_size: usize = raw_records
            .iter()
            .fold(0, |size, record| size + record.size());

        (raw_records, map_size)
    }

    #[quickcheck]
    fn test_read_write(records: Vec<TestRecord>, records_next: Vec<TestRecord>) {
        if records.is_empty() || records_next.is_empty() {
            return;
        }

        let tmp = tempfile::NamedTempFile::new().unwrap();

        let (raw_records, map_size) = convert_records(&records);

        let flatfile = FlatFile::new(tmp.path(), map_size).unwrap();
        flatfile.append(&raw_records).unwrap();

        let mut offset = 0;
        for record in raw_records.iter() {
            let (drive_record, size) = flatfile.get_record_at_offset(offset).unwrap();

            assert_eq!(record.key, drive_record.key());
            assert_eq!(record.value, drive_record.value());

            offset += size;
        }

        let result = flatfile.get_record_at_offset(map_size + 1);
        assert!(result.is_none());

        let (raw_records, _) = convert_records(&records_next);
        let result = flatfile.append(&raw_records);
        assert!(matches!(result, Err(Error::MmapTooSmall)));
    }

    #[quickcheck]
    fn write_two_times_success(records: Vec<TestRecord>, records_next: Vec<TestRecord>) {
        if records.is_empty() || records_next.is_empty() {
            return;
        }

        let tmp = tempfile::NamedTempFile::new().unwrap();

        let (raw_records, map_size) = convert_records(&records);
        let (raw_records_next, map_size_next) = convert_records(&records_next);
        let map_size = map_size + map_size_next;

        let flatfile = FlatFile::new(tmp.path(), map_size).unwrap();
        flatfile.append(&raw_records).unwrap();

        let mut offset = 0;
        for record in raw_records.iter() {
            let (drive_record, size) = flatfile.get_record_at_offset(offset).unwrap();

            assert_eq!(record.key, drive_record.key());
            assert_eq!(record.value, drive_record.value());

            offset += size;
        }

        flatfile.append(&raw_records_next).unwrap();

        for record in raw_records_next.iter() {
            let (drive_record, size) = flatfile.get_record_at_offset(offset).unwrap();

            assert_eq!(record.key, drive_record.key());
            assert_eq!(record.value, drive_record.value());

            offset += size;
        }
    }

    #[quickcheck]
    fn parallel_read_write(records: Vec<TestRecord>, records_next: Vec<TestRecord>) {
        if records.is_empty() || records_next.is_empty() {
            return;
        }

        let tmp = tempfile::NamedTempFile::new().unwrap();

        let (raw_records, map_size_first) = convert_records(&records);
        let (_, map_size_next) = convert_records(&records_next);
        let map_size = map_size_first + map_size_next;

        let flatfile = Arc::new(FlatFile::new(tmp.path(), map_size).unwrap());
        flatfile.append(&raw_records).unwrap();

        let flatfile_write_copy = flatfile.clone();
        let write_thread = std::thread::spawn(move || {
            let (raw_records, _) = convert_records(&records_next);
            flatfile_write_copy.append(&raw_records).unwrap();

            let mut offset = map_size_first;
            for record in raw_records.iter() {
                let (drive_record, size) =
                    flatfile_write_copy.get_record_at_offset(offset).unwrap();

                assert_eq!(record.key, drive_record.key());
                assert_eq!(record.value, drive_record.value());

                offset += size;
            }
        });

        let mut offset = 0;
        for record in raw_records.iter() {
            let (drive_record, size) = flatfile.get_record_at_offset(offset).unwrap();

            assert_eq!(record.key, drive_record.key());
            assert_eq!(record.value, drive_record.value());

            offset += size;
        }

        write_thread.join().unwrap();
    }
}

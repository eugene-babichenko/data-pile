use crate::{Appender, Error, Record, RecordSerializer};
use std::{marker::PhantomData, path::Path};

/// Flatfiles are the main database files that hold all keys and data.
///
/// Records are stored without any additional spaces. The file does not hold any
/// additional data.
///
/// A flatfile is opened with `mmap` and we rely on OS's mechanisms for caching
/// pages, etc.
pub(crate) struct FlatFile<R: RecordSerializer> {
    inner: Appender,
    pd: PhantomData<R>,
}

/// Low-level interface to flatfiles.
impl<R: RecordSerializer> FlatFile<R> {
    /// Open a flatfile.
    ///
    /// # Arguments
    ///
    /// * `path` - the path to the file. It will be created if not exists.
    /// * `map_size` - the size of the memory map that will be used. This map
    ///   limits the size of the file. If the `map_size` is smaller than the
    ///   size of the file, an error will be returned.
    pub fn new<P: AsRef<Path>>(path: P, map_size: usize) -> Result<Self, Error> {
        Appender::new(path, map_size).map(|inner| FlatFile {
            inner,
            pd: PhantomData,
        })
    }

    /// Write an array of records to the drive. This function will block if
    /// another write is still in progress.
    pub fn append(&self, records: &[Record]) -> Result<(), Error> {
        let size_inc: usize = records
            .iter()
            .fold(0, |value, record| value + R::size(&record));

        self.inner.append(size_inc, move |mut mmap| {
            for record in records {
                R::serialize(record, &mut mmap);
                mmap = &mut mmap[R::size(&record)..];
            }
        })
    }

    /// Get the value at the given `offset`. If the `offset` is outside of the
    /// file boundaries, `None` is returned. Upon a successul read a key-value
    /// record is returned. Note that this function do not check if the given
    /// `offset` is the start of an actual record, so you should be careful when
    /// using it.
    pub fn get_record_at_offset(&self, offset: usize) -> Option<Record> {
        self.inner
            .get_data(move |mmap| R::deserialize(&mmap[offset..]))
    }

    pub fn len(&self) -> usize {
        self.inner.size()
    }
}

#[cfg(test)]
mod tests {
    use super::{FlatFile, Record};
    use crate::{
        record::{BasicRecordSerializer, RecordSerializer},
        testutils::TestData,
    };

    fn convert_records(records: &[TestData]) -> (Vec<Record>, usize) {
        let raw_records: Vec<_> = records
            .iter()
            .map(|record| Record::new(&record.key, &record.value))
            .collect();

        let map_size: usize = raw_records.iter().fold(0, |size, record| {
            size + BasicRecordSerializer::size(&record)
        });

        (raw_records, map_size)
    }

    #[quickcheck]
    fn test_read_write(records: Vec<TestData>) {
        if records.is_empty() {
            return;
        }

        let tmp = tempfile::NamedTempFile::new().unwrap();

        let (raw_records, map_size) = convert_records(&records);

        let flatfile = FlatFile::<BasicRecordSerializer>::new(tmp.path(), map_size).unwrap();
        flatfile.append(&raw_records).unwrap();

        let mut offset = 0;
        for record in raw_records.iter() {
            let drive_record = flatfile.get_record_at_offset(offset).unwrap();

            assert_eq!(record.key(), drive_record.key());
            assert_eq!(record.value(), drive_record.value());

            offset += BasicRecordSerializer::size(&record);
        }
    }
}

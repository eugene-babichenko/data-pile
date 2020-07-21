use crate::{Appender, Error, Record, RecordSerializer};
use std::path::Path;

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
    pub fn append<R>(&self, serializer: R, records: &[Record]) -> Result<(), Error>
    where
        R: RecordSerializer,
    {
        let size_inc: usize = records
            .iter()
            .fold(0, |value, record| value + serializer.size(&record));

        self.inner.append(size_inc, move |mut mmap| {
            for record in records {
                serializer.serialize(record, &mut mmap);
                mmap = &mut mmap[serializer.size(&record)..];
            }
        })
    }

    /// Get the value at the given `offset`. If the `offset` is outside of the
    /// file boundaries, `None` is returned. Upon a successul read a key-value
    /// record is returned. Note that this function do not check if the given
    /// `offset` is the start of an actual record, so you should be careful when
    /// using it.
    pub fn get_record_at_offset<R>(&self, serializer: R, offset: usize) -> Option<Record>
    where
        R: RecordSerializer,
    {
        self.inner
            .get_data(offset, move |mmap| serializer.deserialize(mmap))
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
    use super::{FlatFile, Record};
    use crate::{
        serialization::{BasicRecordSerializer, RecordSerializer},
        testutils::TestData,
    };

    #[quickcheck]
    fn test_read_write(records: Vec<TestData>) {
        if records.is_empty() {
            return;
        }

        let tmp = tempfile::NamedTempFile::new().unwrap();

        let raw_records: Vec<_> = records
            .iter()
            .map(|record| Record::new(&record.key, &record.value))
            .collect();

        let flatfile = FlatFile::new(tmp.path()).unwrap();
        flatfile
            .append(BasicRecordSerializer, &raw_records)
            .unwrap();

        let mut offset = 0;
        for record in raw_records.iter() {
            let drive_record = flatfile
                .get_record_at_offset(BasicRecordSerializer, offset)
                .unwrap();

            assert_eq!(record.key(), drive_record.key());
            assert_eq!(record.value(), drive_record.value());

            offset += BasicRecordSerializer.size(&record);
        }
    }
}

use crate::{flatfile::FlatFile, seqno::SeqNoIndex, Error, Record, RecordSerializer};
use std::path::Path;

// 4 GiB
pub const DEFAULT_FLATFILE_MAP_SIZE: usize = (1 << 30) * 4;
// 512 MiB
pub const DEFAULT_SEQNO_INDEX_MAP_SIZE: usize = (1 << 20) * 512;

/// Build `Database` instances.
pub struct DatabaseBuilder {
    flatfile_map_size: usize,
    seqno_index_map_size: usize,
}

pub struct Database<R: RecordSerializer> {
    flatfile: FlatFile,
    seqno_index: SeqNoIndex,
    serializer: R,
}

impl DatabaseBuilder {
    /// Create a builder with the default parameters.
    pub fn new() -> Self {
        Self {
            flatfile_map_size: DEFAULT_FLATFILE_MAP_SIZE,
            seqno_index_map_size: DEFAULT_SEQNO_INDEX_MAP_SIZE,
        }
    }

    /// The size of `mmap` range to be used for reading database files.
    pub fn flatfile_map_size(self, value: usize) -> Self {
        Self {
            flatfile_map_size: value,
            ..self
        }
    }

    /// The size of `mmap` range to be used for reading the sequential number index.
    pub fn seqno_index_map_size(self, value: usize) -> Self {
        Self {
            seqno_index_map_size: value,
            ..self
        }
    }

    /// Open the database. Will create it if not exists.
    pub fn open<P, R>(self, path: P, serializer: R) -> Result<Database<R>, Error>
    where
        P: AsRef<Path>,
        R: RecordSerializer,
    {
        let path = path.as_ref();

        if !path.is_dir() {
            return Err(Error::PathNotDir);
        }

        if !path.exists() {
            std::fs::create_dir(path).map_err(|err| Error::FileOpen(path.to_path_buf(), err))?;
        }

        let flatfile_path = path.join("data");
        let flatfile = FlatFile::new(flatfile_path, self.flatfile_map_size)?;

        let seqno_index_path = path.join("seqno");
        let seqno_index = SeqNoIndex::new(seqno_index_path, self.seqno_index_map_size)?;

        Ok(Database {
            flatfile,
            seqno_index,
            serializer,
        })
    }
}

impl<R: RecordSerializer> Database<R> {
    /// Write an array of records to the database. This function will block if
    /// another write is still in progress.
    pub fn append(&self, records: &[Record]) -> Result<(), Error> {
        let initial_size = self.flatfile.len();

        self.flatfile.append(&self.serializer, records)?;

        let (_, seqno_index_update) = records.iter().fold(
            (initial_size, Vec::with_capacity(records.len())),
            |(offset, mut update), record| {
                update.push(offset as u64);
                (offset + self.serializer.size(&record), update)
            },
        );

        println!("{:?}", seqno_index_update);

        self.seqno_index.append(&seqno_index_update)?;

        Ok(())
    }

    /// Get a record by its sequential number.
    pub fn get_by_seqno(&self, seqno: usize) -> Option<Record> {
        let offset = self.seqno_index.get_pointer_to_value(seqno)?;
        self.flatfile
            .get_record_at_offset(&self.serializer, offset as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::DatabaseBuilder;
    use crate::{record::BasicRecordSerializer, testutils::TestData, Record, RecordSerializer};

    #[quickcheck]
    fn read_write(data: Vec<TestData>) {
        if data.is_empty() {
            return;
        }

        let tmp = tempfile::tempdir().unwrap();

        let records: Vec<_> = data
            .iter()
            .map(|data| Record::new(&data.key, &data.value))
            .collect();
        let flatfile_size = records
            .iter()
            .fold(0, |size, record| size + BasicRecordSerializer.size(&record));
        let seqno_index_size = records.len() * std::mem::size_of::<u64>();

        let db = DatabaseBuilder::new()
            .flatfile_map_size(flatfile_size)
            .seqno_index_map_size(seqno_index_size)
            .open(tmp.path(), BasicRecordSerializer)
            .unwrap();

        db.append(&records).unwrap();

        for i in 0..records.len() {
            let record = db.get_by_seqno(i).unwrap();
            assert_eq!(records[i].key(), record.key());
            assert_eq!(records[i].value(), record.value());
        }
    }
}

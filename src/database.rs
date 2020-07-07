use crate::{
    flatfile::FlatFile, index::Index, seqno::SeqNoIndex, Error, Record, RecordSerializer, SeqNoIter,
};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

// 4 GiB
pub const DEFAULT_FLATFILE_MAP_SIZE: usize = (1 << 30) * 4;
// 512 MiB
pub const DEFAULT_SEQNO_INDEX_MAP_SIZE: usize = (1 << 20) * 512;

/// Build `Database` instances.
pub struct DatabaseBuilder {
    flatfile_map_size: usize,
    seqno_index_map_size: usize,
}

#[derive(Clone)]
pub struct Database<R: RecordSerializer + Clone> {
    flatfile: Arc<FlatFile>,
    seqno_index: Arc<SeqNoIndex>,
    index: Index,
    serializer: R,
    write_lock: Arc<Mutex<()>>,
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
        R: RecordSerializer + Clone,
    {
        Database::new(
            path,
            serializer,
            self.flatfile_map_size,
            self.seqno_index_map_size,
        )
    }
}

impl<R: RecordSerializer + Clone> Database<R> {
    fn new<P>(
        path: P,
        serializer: R,
        flatfile_map_size: usize,
        seqno_index_map_size: usize,
    ) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();

        if !path.is_dir() {
            return Err(Error::PathNotDir);
        }

        if !path.exists() {
            std::fs::create_dir(path).map_err(|err| Error::FileOpen(path.to_path_buf(), err))?;
        }

        let flatfile_path = path.join("data");
        let flatfile = Arc::new(FlatFile::new(flatfile_path, flatfile_map_size)?);

        let seqno_index_path = path.join("seqno");
        let seqno_index = Arc::new(SeqNoIndex::new(seqno_index_path, seqno_index_map_size)?);

        let mut test_iter = SeqNoIter::new(flatfile.clone(), serializer.clone(), 0);

        let mut data_bytes_read: usize = 0;
        let mut seqno_bytes_read = 0;
        let mut seqno_finished = false;
        let mut records_read = 0;
        let mut seqno_index_update = Vec::new();

        while let Some(record) = test_iter.next() {
            match seqno_index.get_pointer_to_value(records_read) {
                Some(pointer) => {
                    if pointer != data_bytes_read as u64 {
                        return Err(Error::SeqNoIndexDamaged);
                    }
                    seqno_bytes_read += std::mem::size_of::<u64>();
                }
                None => {
                    if seqno_finished {
                        seqno_index_update.push(data_bytes_read as u64)
                    } else {
                        if seqno_bytes_read != seqno_index.len() {
                            return Err(Error::SeqNoIndexDamaged);
                        }
                        seqno_finished = true;
                        seqno_index_update.push(data_bytes_read as u64)
                    }
                }
            }
            data_bytes_read += serializer.size(&record);
            records_read += 1;
        }

        if data_bytes_read < flatfile.len() as usize {
            return Err(Error::DataFileDamaged);
        }

        seqno_index.append(&seqno_index_update)?;

        let index = Index::new(flatfile.clone(), serializer.clone());

        let write_lock = Arc::new(Mutex::new(()));

        Ok(Database {
            flatfile,
            seqno_index,
            index,
            serializer,
            write_lock,
        })
    }

    /// Write an array of records to the database. This function will block if
    /// another write is still in progress.
    pub fn append(&self, records: &[Record]) -> Result<(), Error> {
        let _write_guard = self.write_lock.lock().unwrap();

        for record in records.iter() {
            if self.index.contains(record.key()) {
                return Err(Error::RecordExists(record.key().to_vec()));
            }
        }

        let initial_size = self.flatfile.len();

        self.flatfile.append(&self.serializer, records)?;

        let mut seqno_index_update = Vec::with_capacity(records.len());
        let mut index_update = Vec::with_capacity(records.len());
        let mut offset = initial_size;

        for record in records.iter() {
            seqno_index_update.push(offset as u64);
            index_update.push((record.key(), offset));
            offset += self.serializer.size(record);
        }

        self.seqno_index.append(&seqno_index_update)?;
        self.index.append(&index_update);

        Ok(())
    }

    pub fn put(&self, record: Record) -> Result<(), Error> {
        self.append(&[record])
    }

    /// Get a record by its key.
    pub fn get(&self, key: &[u8]) -> Option<Record> {
        let offset = self.index.get(key)?;
        self.flatfile.get_record_at_offset(&self.serializer, offset)
    }

    /// Get a record by its sequential number.
    pub fn get_by_seqno(&self, seqno: usize) -> Option<Record> {
        let offset = self.seqno_index.get_pointer_to_value(seqno)?;
        self.flatfile
            .get_record_at_offset(&self.serializer, offset as usize)
    }

    /// Iterate records in the order they were added starting form the given
    /// sequential number.
    pub fn iter_from_seqno(&self, seqno: usize) -> Option<SeqNoIter<R>> {
        let offset = self.seqno_index.get_pointer_to_value(seqno)? as usize;
        Some(SeqNoIter::new(
            self.flatfile.clone(),
            self.serializer.clone(),
            offset,
        ))
    }

    /// Get the underlying raw data. You can then use this data to recover a
    /// database (for example, on another machine). To recover you will need to
    /// write the snapshot data to a file `<database path>/data`.
    pub fn snapshot(&self) -> &[u8] {
        self.flatfile.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::DatabaseBuilder;
    use crate::{
        serialization::BasicRecordSerializer, testutils::TestData, Record, RecordSerializer,
    };

    #[quickcheck]
    fn read_write(mut data: Vec<TestData>) {
        if data.is_empty() {
            return;
        }

        data.sort_by_key(|record| record.key.to_owned());
        data.dedup_by_key(|record| record.key.to_owned());

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

        for i in 0..records.len() {
            let record = db.get(records[i].key()).unwrap();
            assert_eq!(records[i].key(), record.key());
            assert_eq!(records[i].value(), record.value());
        }

        let mut iter = db.iter_from_seqno(0).unwrap();
        let mut count = 0;

        while let Some(record) = iter.next() {
            assert_eq!(records[count].key(), record.key());
            assert_eq!(records[count].value(), record.value());
            count += 1;
        }
        assert_eq!(count, records.len());

        // test snapshot recovery

        let tmp_new = tempfile::tempdir().unwrap();

        let data_path_old = tmp.path().join("data");
        let data_path_new = tmp_new.path().join("data");
        std::fs::copy(data_path_old, data_path_new).unwrap();

        let db_new = DatabaseBuilder::new()
            .flatfile_map_size(flatfile_size)
            .seqno_index_map_size(seqno_index_size)
            .open(tmp_new.path(), BasicRecordSerializer)
            .unwrap();

        for i in 0..records.len() {
            let record = db_new.get_by_seqno(i).unwrap();
            assert_eq!(records[i].key(), record.key());
            assert_eq!(records[i].value(), record.value());
        }

        for i in 0..records.len() {
            let record = db_new.get(records[i].key()).unwrap();
            assert_eq!(records[i].key(), record.key());
            assert_eq!(records[i].value(), record.value());
        }
    }
}

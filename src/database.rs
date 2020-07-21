use crate::{
    flatfile::FlatFile, index::Index, seqno::SeqNoIndex, Error, Record, RecordSerializer, SeqNoIter,
};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct Database<R: RecordSerializer + Clone> {
    flatfile: Arc<FlatFile>,
    seqno_index: Arc<SeqNoIndex>,
    index: Index,
    serializer: R,
    write_lock: Arc<Mutex<()>>,
}

impl<R: RecordSerializer + Clone> Database<R> {
    pub fn new<P>(path: P, serializer: R) -> Result<Self, Error>
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
        let flatfile = Arc::new(FlatFile::new(flatfile_path)?);

        let seqno_index_path = path.join("seqno");
        let seqno_index = Arc::new(SeqNoIndex::new(seqno_index_path)?);

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

    // /// Get the underlying raw data. You can then use this data to recover a
    // /// database (for example, on another machine). To recover you will need to
    // /// write the snapshot data to a file `<database path>/data`.
    // pub fn snapshot(&self) -> &[u8] {
    //     self.flatfile.snapshot()
    // }
}

#[cfg(test)]
mod tests {
    use super::Database;
    use crate::{serialization::BasicRecordSerializer, testutils::TestData, Record};
    use std::collections::HashSet;

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

        let db = Database::new(tmp.path(), BasicRecordSerializer).unwrap();

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

        let db_new = Database::new(tmp_new.path(), BasicRecordSerializer).unwrap();

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

    #[quickcheck]
    fn parallel_read_write(mut data1: Vec<TestData>, data2: Vec<TestData>) {
        if data1.is_empty() || data2.is_empty() {
            return;
        }

        data1.sort_by_key(|record| record.key.to_owned());
        data1.dedup_by_key(|record| record.key.to_owned());

        let data1_keys: HashSet<_> = data1.iter().map(|record| &record.key).collect();
        let mut data2: Vec<_> = data2
            .into_iter()
            .filter(|record| !data1_keys.contains(&record.key))
            .collect();

        let records1: Vec<_> = data1
            .iter()
            .map(|data| Record::new(&data.key, &data.value))
            .collect();

        let tmp = tempfile::tempdir().unwrap();
        let db = Database::new(tmp.path(), BasicRecordSerializer).unwrap();

        db.append(&records1).unwrap();

        let write_db = db.clone();

        let write_thread = std::thread::spawn(move || {
            data2.sort_by_key(|record| record.key.to_owned());
            data2.dedup_by_key(|record| record.key.to_owned());

            let records2: Vec<_> = data2
                .iter()
                .map(|data| Record::new(&data.key, &data.value))
                .collect();

            write_db.append(&records2).unwrap();

            data2
        });

        for i in 0..records1.len() {
            let record = db.get_by_seqno(i).unwrap();
            assert_eq!(records1[i].key(), record.key());
            assert_eq!(records1[i].value(), record.value());
        }

        let data2 = write_thread.join().unwrap();

        for i in data1.len()..(data1.len() + data2.len()) {
            let record = db.get_by_seqno(i).unwrap();
            let i = i - data1.len();
            assert_eq!(data2[i].key, record.key());
            assert_eq!(data2[i].value, record.value());
        }
    }
}

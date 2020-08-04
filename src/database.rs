use crate::{flatfile::FlatFile, seqno::SeqNoIndex, Error, SeqNoIter};
use std::{
    mem::size_of,
    path::Path,
    sync::{Arc, Mutex},
};

/// Append-only database. Can be safely cloned and used from different threads.
#[derive(Clone)]
pub struct Database {
    flatfile: Arc<FlatFile>,
    seqno_index: Arc<SeqNoIndex>,
    write_lock: Arc<Mutex<()>>,
}

impl Database {
    /// Open the database. Will create one if not exists.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let path = path.as_ref();

        if !path.exists() {
            std::fs::create_dir(path).map_err(|err| Error::FileOpen(path.to_path_buf(), err))?;
        }

        if !path.is_dir() {
            return Err(Error::PathNotDir);
        }

        let flatfile_path = path.join("data");
        let flatfile = Arc::new(FlatFile::new(flatfile_path)?);

        let seqno_index_path = path.join("seqno");
        let seqno_index = Arc::new(SeqNoIndex::new(seqno_index_path)?);

        let mut test_iter = SeqNoIter::new(flatfile.clone(), 0);

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
            data_bytes_read += record.len() + size_of::<u64>();
            records_read += 1;
        }

        if data_bytes_read < flatfile.len() as usize {
            return Err(Error::DataFileDamaged);
        }

        seqno_index.append(&seqno_index_update)?;

        let write_lock = Arc::new(Mutex::new(()));

        Ok(Database {
            flatfile,
            seqno_index,
            write_lock,
        })
    }

    /// Write an array of records to the database. This function will block if
    /// another write is still in progress.
    pub fn append(&self, records: &[&[u8]]) -> Result<(), Error> {
        let _write_guard = self.write_lock.lock().unwrap();

        let initial_size = self.flatfile.len();

        self.flatfile.append(records)?;

        let mut seqno_index_update = Vec::with_capacity(records.len());
        let mut offset = initial_size;

        for record in records.iter() {
            seqno_index_update.push(offset as u64);
            offset += record.len() + size_of::<u64>();
        }

        self.seqno_index.append(&seqno_index_update)?;

        Ok(())
    }

    /// Put a single record (not recommended).
    pub fn put(&self, record: &[u8]) -> Result<(), Error> {
        self.append(&[record])
    }

    /// Get a record by its sequential number.
    pub fn get_by_seqno(&self, seqno: usize) -> Option<&[u8]> {
        let offset = self.seqno_index.get_pointer_to_value(seqno)?;
        self.flatfile.get_record_at_offset(offset as usize)
    }

    /// Iterate records in the order they were added starting form the given
    /// sequential number.
    pub fn iter_from_seqno(&self, seqno: usize) -> Option<SeqNoIter> {
        let offset = self.seqno_index.get_pointer_to_value(seqno)? as usize;
        Some(SeqNoIter::new(self.flatfile.clone(), offset))
    }
    /// Get the underlying raw data. You can then use this data to recover a
    /// database (for example, on another machine). To recover you will need to
    /// write the snapshot data to a file `<database path>/data`.
    pub fn snapshot(&self) -> Result<impl AsRef<[u8]>, Error> {
        self.flatfile.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::Database;

    #[quickcheck]
    fn read_write(data: Vec<Vec<u8>>) {
        if data.is_empty() {
            return;
        }

        let tmp = tempfile::tempdir().unwrap();

        let records: Vec<_> = data.iter().map(|data| data.as_ref()).collect();

        let db = Database::new(tmp.path()).unwrap();

        db.append(&records).unwrap();

        for i in 0..records.len() {
            let record = db.get_by_seqno(i).unwrap();
            assert_eq!(records[i], record);
        }

        let mut iter = db.iter_from_seqno(0).unwrap();
        let mut count = 0;

        while let Some(record) = iter.next() {
            assert_eq!(records[count], record);
            count += 1;
        }
        assert_eq!(count, records.len());

        // test snapshot recovery

        let tmp_new = tempfile::tempdir().unwrap();

        let data_path_old = tmp.path().join("data");
        let data_path_new = tmp_new.path().join("data");
        std::fs::copy(data_path_old, data_path_new).unwrap();

        let db_new = Database::new(tmp_new.path()).unwrap();

        for i in 0..records.len() {
            let record = db_new.get_by_seqno(i).unwrap();
            assert_eq!(records[i], record);
        }
    }

    #[quickcheck]
    fn parallel_read_write(data1: Vec<Vec<u8>>, data2: Vec<Vec<u8>>) {
        if data1.is_empty() || data2.is_empty() {
            return;
        }

        let records1: Vec<_> = data1.iter().map(|data| data.as_ref()).collect();

        let tmp = tempfile::tempdir().unwrap();
        let db = Database::new(tmp.path()).unwrap();

        db.append(&records1).unwrap();

        let write_db = db.clone();

        let write_thread = std::thread::spawn(move || {
            let records2: Vec<_> = data2.iter().map(|data| data.as_ref()).collect();

            write_db.append(&records2).unwrap();

            data2
        });

        for i in 0..records1.len() {
            let record = db.get_by_seqno(i).unwrap();
            assert_eq!(records1[i], record);
        }

        let data2 = write_thread.join().unwrap();

        for i in data1.len()..(data1.len() + data2.len()) {
            let record = db.get_by_seqno(i).unwrap();
            let i = i - data1.len();
            assert_eq!(data2[i], record);
        }
    }
}

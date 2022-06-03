use crate::{flatfile::FlatFile, seqno::SeqNoIndex, Error, SeqNoIter};
use std::{
    path::{Path, PathBuf},
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
    pub fn file<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let path = path.as_ref();

        if !path.exists() {
            std::fs::create_dir(path).map_err(|err| Error::FileOpen(path.to_path_buf(), err))?;
        }

        if !path.is_dir() {
            return Err(Error::PathNotDir);
        }

        let flatfile_path = path.join("data");
        let seqno_index_path = path.join("seqno");

        Self::new(Some(flatfile_path), Some(seqno_index_path), true)
    }

    /// Open the database. Will create one if not exists.
    pub fn file_readonly<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let path = path.as_ref();

        if !path.exists() {
            return Err(Error::PathNotFound);
        }

        if !path.is_dir() {
            return Err(Error::PathNotDir);
        }

        let flatfile_path = path.join("data");
        let seqno_index_path = path.join("seqno");

        Self::new(Some(flatfile_path), Some(seqno_index_path), false)
    }

    /// Open an in-memory database.
    pub fn memory() -> Result<Self, Error> {
        Self::new(None, None, true)
    }

    pub(crate) fn new(
        flatfile_path: Option<PathBuf>,
        seqno_index_path: Option<PathBuf>,
        writable: bool,
    ) -> Result<Self, Error> {
        let flatfile = Arc::new(FlatFile::new(flatfile_path, writable)?);
        let seqno_index = Arc::new(SeqNoIndex::new(seqno_index_path, writable)?);

        let seqno_size = seqno_index.size();
        if seqno_size > 0
            && seqno_index
                .get_pointer_to_value(seqno_size - 1)
                .map(|pos| pos >= flatfile.memory_size() as u64)
                .unwrap_or(true)
        {
            return Err(Error::SeqNoIndexDamaged);
        }

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
        self.append_get_seqno(records).map(|_| ())
    }

    /// Write an array of records to the database. This function will block if
    /// another write is still in progress.
    pub fn append_get_seqno(&self, records: &[&[u8]]) -> Result<Option<usize>, Error> {
        if records.is_empty() {
            return Ok(None);
        }

        let _write_guard = self.write_lock.lock().unwrap();

        let initial_size = self.flatfile.memory_size();

        let mut seqno_index_update = Vec::with_capacity(records.len());
        let mut offset = initial_size;

        for record in records.iter() {
            seqno_index_update.push(offset as u64);
            offset += record.len();
        }

        let seqno = self.seqno_index.append(&seqno_index_update)?;
        self.flatfile.append(records)?;

        Ok(seqno)
    }

    /// Put a single record (not recommended).
    pub fn put(&self, record: &[u8]) -> Result<(), Error> {
        self.append(&[record])
    }

    /// Get a record by its sequential number.
    pub fn get_by_seqno(&self, seqno: usize) -> Option<Vec<u8>> {
        let offset = self.seqno_index.get_pointer_to_value(seqno)? as usize;
        let next_offset = self
            .seqno_index
            .get_pointer_to_value(seqno + 1)
            .map(|value| value as usize)
            .unwrap_or_else(|| self.flatfile.memory_size());
        let length = next_offset - offset;
        self.flatfile.get_record_at_offset(offset, length)
    }

    /// Iterate records in the order they were added starting form the given
    /// sequential number.
    pub fn iter_from_seqno(&self, seqno: usize) -> Option<SeqNoIter> {
        Some(SeqNoIter::new(
            self.flatfile.clone(),
            self.seqno_index.clone(),
            seqno,
        ))
    }

    pub fn last(&self) -> Option<Vec<u8>> {
        self.get_by_seqno(self.len().saturating_sub(1))
    }

    pub fn len(&self) -> usize {
        self.seqno_index.size()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::Database;

    fn read_write(db: Database, data1: Vec<Vec<u8>>, data2: Vec<Vec<u8>>) {
        let records1: Vec<_> = data1
            .iter()
            .filter(|data| !data.is_empty())
            .map(|data| data.as_ref())
            .collect();
        let records2: Vec<_> = data2
            .iter()
            .filter(|data| !data.is_empty())
            .map(|data| data.as_ref())
            .collect();

        if data1.is_empty() || data2.is_empty() {
            return;
        }

        db.append(&records1).unwrap();

        for (i, original_record) in records1.iter().enumerate() {
            let record = db.get_by_seqno(i).unwrap();
            assert_eq!(*original_record, record.as_slice());
        }

        assert_eq!(*records1.last().unwrap(), db.last().unwrap().as_slice());
        assert_eq!(records1.len(), db.len());

        let iter = db.iter_from_seqno(0).unwrap();
        let mut count = 0;

        for record in iter {
            assert_eq!(records1[count], record.as_slice());
            count += 1;
        }
        assert_eq!(count, records1.len());

        assert_eq!(
            db.append_get_seqno(&records2).unwrap().unwrap(),
            records1.len()
        );

        for i in records1.len()..(records1.len() + records2.len()) {
            let record = db.get_by_seqno(i).unwrap();
            assert_eq!(records2[i - records1.len()], record.as_slice());
        }

        assert_eq!(*records2.last().unwrap(), db.last().unwrap().as_slice());
        assert_eq!(records1.len() + records2.len(), db.len());
    }

    #[quickcheck]
    fn read_write_memory(data1: Vec<Vec<u8>>, data2: Vec<Vec<u8>>) {
        let db = Database::memory().unwrap();
        read_write(db, data1, data2);
    }

    #[quickcheck]
    fn read_write_storage(data1: Vec<Vec<u8>>, data2: Vec<Vec<u8>>) {
        let tmp = tempfile::tempdir().unwrap();
        let db = Database::file(tmp.path()).unwrap();
        read_write(db, data1, data2);
    }

    fn parallel_read_write(db: Database, data1: Vec<Vec<u8>>, data2: Vec<Vec<u8>>) {
        let data1: Vec<_> = data1.into_iter().filter(|data| !data.is_empty()).collect();
        let data2: Vec<_> = data2.into_iter().filter(|data| !data.is_empty()).collect();

        if data1.is_empty() || data2.is_empty() {
            return;
        }

        let records1: Vec<_> = data1.iter().map(|data| data.as_ref()).collect();

        db.append(&records1).unwrap();

        let write_db = db.clone();

        let write_thread = std::thread::spawn(move || {
            let records2: Vec<&[u8]> = data2.iter().map(|data| data.as_ref()).collect();
            write_db.append(&records2).unwrap();
            data2
        });

        for (i, original_record) in records1.iter().enumerate() {
            let record = db.get_by_seqno(i).unwrap();
            assert_eq!(*original_record, record.as_slice());
        }

        let data2 = write_thread.join().unwrap();

        for i in data1.len()..(data1.len() + data2.len()) {
            let record = db.get_by_seqno(i).unwrap();
            let i = i - data1.len();
            assert_eq!(data2[i], record.as_slice());
        }
    }

    #[quickcheck]
    fn parallel_read_write_memory(data1: Vec<Vec<u8>>, data2: Vec<Vec<u8>>) {
        let db = Database::memory().unwrap();
        parallel_read_write(db, data1, data2);
    }

    #[quickcheck]
    fn parallel_read_write_storage(data1: Vec<Vec<u8>>, data2: Vec<Vec<u8>>) {
        let tmp = tempfile::tempdir().unwrap();
        let db = Database::file(tmp.path()).unwrap();
        parallel_read_write(db, data1, data2);
    }

    fn big_write_test(db: Database, size_one_record: u64, write_count: u64) {
        let record: Vec<u8> = (0..size_one_record).map(|i| (i % 256) as u8).collect();
        let records = vec![record.as_slice()];

        for _ in 0..write_count {
            db.append(&records).unwrap();
        }

        for i in 0..write_count {
            let found = db.get_by_seqno(i as usize).unwrap();
            assert_eq!(record, found);
        }
    }

    fn reopen_test(db: Database, size_one_record: u64, write_count: u64, iteration: u64) {
        let record: Vec<u8> = (0..size_one_record).map(|i| (i % 256) as u8).collect();
        let records = vec![record.as_slice()];

        assert_eq!(iteration * write_count, db.len() as u64);
        for i in 0..write_count {
            db.append(&records).unwrap();
            assert_eq!(iteration * write_count + i + 1, db.len() as u64);
        }

        for i in 0..(iteration + 1) * write_count {
            let found = db.get_by_seqno(i as usize).unwrap();
            assert_eq!(record, found);
        }

        assert_eq!((iteration + 1) * write_count, db.len() as u64);
        drop(db);
    }

    #[test]
    fn big_write_memory() {
        let one_record_size = 1024;
        let records = 100000;
        let db = Database::memory().unwrap();
        big_write_test(db, one_record_size, records);
    }

    #[test]
    fn backup_test_storage() {
        let one_record_size = 1024;
        let records = 10000;

        let tmp = tempfile::tempdir().unwrap();
        for iteration in 0..5 {
            let db = Database::file(tmp.path()).unwrap();
            reopen_test(db, one_record_size, records, iteration);
        }
    }

    #[test]
    fn big_write_storage() {
        let tmp = tempfile::tempdir().unwrap();
        let db = Database::file(tmp.path()).unwrap();

        let one_record_size = 1024;
        let records = 50000;
        big_write_test(db, one_record_size, records);
    }
}

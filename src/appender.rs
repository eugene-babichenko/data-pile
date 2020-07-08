//! Appenders are mmap'ed files intended for append-only use.

use crate::Error;
use memmap::{MmapMut, MmapOptions};
use std::{
    cell::UnsafeCell,
    fs::{File, OpenOptions},
    marker::Sync,
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
};

pub(crate) struct Appender {
    file: File,
    // This is used to trick the compiler so that we have parallel reads and
    // writes.
    mmap: UnsafeCell<MaybeMmap>,
    // Atomic is used to ensure that we can have lock-free and memory-safe
    // reads. Since this value is updated only after the write has finished it
    // is safe to use it as the upper boundary for reads.
    actual_size: AtomicUsize,
}

/// This enum is required to handle a special case: on Windows we cannot have a
/// zero-sized memory map, so we need to have a file of non-zero length first.
// TODO relace this with a union when the corresponding RFC is stabilized.
// https://github.com/rust-lang/rust/issues/55149
enum MaybeMmap {
    Mmap(MmapMut),
    Uninit(usize),
}

impl Appender {
    /// Open a flatfile.
    ///
    /// # Arguments
    ///
    /// * `path` - the path to the file. It will be created if not exists.
    /// * `map_size` - the size of the memory map that will be used. This map
    ///   limits the size of the file. If the `map_size` is smaller than the
    ///   size of the file, an error will be returned.
    pub fn new<P: AsRef<Path>>(path: P, map_size: usize) -> Result<Self, Error> {
        let path = path.as_ref();

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)
            .map_err(|err| Error::FileOpen(path.to_path_buf(), err))?;

        let actual_size = file
            .metadata()
            .map_err(|err| Error::FileOpen(path.to_path_buf(), err))?
            .len() as usize;

        if map_size < actual_size {
            return Err(Error::MmapTooSmall);
        }

        let mut mmap = MaybeMmap::new_uninit(map_size);
        if actual_size > 0 {
            mmap.init(&file)?;
        }
        let mmap = UnsafeCell::new(mmap);

        let actual_size = AtomicUsize::from(actual_size);

        Ok(Self {
            file,
            mmap,
            actual_size,
        })
    }

    /// Append data to the file. The mutable pointer to the new data location is
    /// given to `f` which should write the data. This function will block if
    /// another write is in progress.
    pub fn append<F>(&self, size_inc: usize, f: F) -> Result<(), Error>
    where
        F: Fn(&mut [u8]),
    {
        let maybe_mmap = unsafe { self.mmap.get().as_mut().unwrap() };
        let actual_size = self.actual_size.load(Ordering::Relaxed);

        let new_file_size = actual_size + size_inc;
        if maybe_mmap.len() < new_file_size {
            return Err(Error::MmapTooSmall);
        }

        let result = {
            self.file
                .set_len(new_file_size as u64)
                .map_err(Error::Write)?;
            maybe_mmap.init(&self.file)?;

            let mmap = maybe_mmap.as_mut().unwrap();

            f(&mut mmap[actual_size..new_file_size]);

            mmap.flush().map_err(Error::Write)?;

            Ok(())
        };

        if let Err(err) = result {
            self.file
                .set_len(actual_size as u64)
                .expect("could not revert unsuccessful append");
            return Err(err);
        }

        self.actual_size.store(new_file_size, Ordering::Relaxed);

        Ok(())
    }

    /// The whole data buffer is given to `f` which should return the data back
    /// or return None if something went wrong.
    pub fn get_data<'a, F, U>(&'a self, f: F) -> Option<U>
    where
        F: Fn(&'a [u8]) -> Option<U>,
    {
        let mmap = unsafe { self.mmap.get().as_ref().unwrap() }.as_ref()?;
        let actual_size = self.actual_size.load(Ordering::Relaxed);

        f(&mmap[0..actual_size])
    }

    pub fn size(&self) -> usize {
        self.actual_size.load(Ordering::Relaxed)
    }

    /// Get the pointer to the underlying raw data.
    pub fn snapshot(&self) -> &[u8] {
        let mmap = unsafe { self.mmap.get().as_ref().unwrap() }
            .as_ref()
            .unwrap_or(&[]);
        let size = self.actual_size.load(Ordering::Relaxed);
        &mmap[..size]
    }
}

unsafe impl Sync for Appender {}

impl MaybeMmap {
    fn new_uninit(map_size: usize) -> Self {
        MaybeMmap::Uninit(map_size)
    }

    fn init(&mut self, file: &File) -> Result<(), Error> {
        match self {
            MaybeMmap::Mmap(_) => {}
            MaybeMmap::Uninit(map_size) => {
                *self = MaybeMmap::Mmap(
                    unsafe { MmapOptions::new().len(*map_size).map_mut(&file) }
                        .map_err(|err| Error::Mmap(err))?,
                );
            }
        }
        Ok(())
    }

    fn as_ref(&self) -> Option<&[u8]> {
        match self {
            MaybeMmap::Mmap(mmap) => Some(mmap),
            MaybeMmap::Uninit(_) => None,
        }
    }

    fn as_mut(&mut self) -> Option<&mut MmapMut> {
        match self {
            MaybeMmap::Mmap(mmap) => Some(mmap),
            MaybeMmap::Uninit(_) => None,
        }
    }

    fn len(&self) -> usize {
        match self {
            MaybeMmap::Mmap(mmap) => mmap.len(),
            MaybeMmap::Uninit(map_size) => *map_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Appender;
    use crate::Error;
    use std::sync::Arc;

    #[quickcheck]
    fn test_read_write(data1: Vec<u8>, data2: Vec<u8>) {
        if data1.is_empty() || data2.is_empty() {
            return;
        }

        let tmp = tempfile::NamedTempFile::new().unwrap();

        let map_size = data1.len();
        let flatfile = Appender::new(tmp.path(), map_size).unwrap();
        flatfile
            .append(data1.len(), |mmap| mmap.copy_from_slice(data1.as_ref()))
            .unwrap();

        let actual_data = flatfile.get_data(|mmap| Some(mmap)).unwrap();
        assert_eq!(data1, actual_data);

        let result = flatfile.append(data2.len(), |mmap| mmap.copy_from_slice(data2.as_ref()));
        assert!(matches!(result, Err(Error::MmapTooSmall)));
    }

    #[quickcheck]
    fn write_two_times_success(mut data1: Vec<u8>, data2: Vec<u8>) {
        if data1.is_empty() || data2.is_empty() {
            return;
        }

        let tmp = tempfile::NamedTempFile::new().unwrap();

        let map_size = data1.len() + data2.len();
        let flatfile = Appender::new(tmp.path(), map_size).unwrap();
        flatfile
            .append(data1.len(), |mmap| mmap.copy_from_slice(data1.as_ref()))
            .unwrap();

        let actual_data = flatfile.get_data(|mmap| Some(mmap)).unwrap();
        assert_eq!(data1, actual_data);

        flatfile
            .append(data2.len(), |mmap| mmap.copy_from_slice(&data2))
            .unwrap();

        let actual_data = flatfile.get_data(|mmap| Some(mmap)).unwrap();
        data1.extend_from_slice(&data2);
        assert_eq!(data1, actual_data);
    }

    #[quickcheck]
    fn parallel_read_write(data1: Vec<u8>, data2: Vec<u8>) {
        if data1.is_empty() || data2.is_empty() {
            return;
        }

        let tmp = tempfile::NamedTempFile::new().unwrap();

        let map_size = data1.len() + data2.len();
        let flatfile = Arc::new(Appender::new(tmp.path(), map_size).unwrap());
        flatfile
            .append(data1.len(), |mmap| mmap.copy_from_slice(data1.as_ref()))
            .unwrap();

        let write_flatfile = flatfile.clone();
        let write_expected = {
            let mut data = data1.clone();
            data.extend_from_slice(&data2);
            data
        };
        let write_thread = std::thread::spawn(move || {
            write_flatfile
                .append(data2.len(), |mmap| mmap.copy_from_slice(data2.as_ref()))
                .unwrap();

            let actual_data = write_flatfile.get_data(|mmap| Some(mmap)).unwrap();
            assert_eq!(write_expected, actual_data);
        });

        let actual_data = flatfile.get_data(|mmap| Some(mmap)).unwrap();
        assert_eq!(data1, actual_data);

        write_thread.join().unwrap();
    }
}

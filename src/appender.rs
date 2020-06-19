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
    // writes. Unfortunately, this also makes `append` non-threadsafe.
    mmap: UnsafeCell<MmapMut>,
    // Atomic is used to ensure that we can have lock-free and memory-safe
    // reads. Since this value is updated only after the write has finished it
    // is safe to use it as the upper boundary for reads.
    actual_size: AtomicUsize,
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

        let mmap = UnsafeCell::new(unsafe {
            MmapOptions::new()
                .len(map_size)
                .map_mut(&file)
                .map_err(|err| Error::Mmap(path.to_path_buf(), err))?
        });

        let actual_size = AtomicUsize::from(actual_size);

        Ok(Self {
            file,
            mmap,
            actual_size,
        })
    }

    /// Append data to the file. The mutable pointer to the new data location is
    /// given to `f` which should write the data. This function is not thread
    /// safe.
    pub fn append<F>(&self, size_inc: usize, f: F) -> Result<(), Error>
    where
        F: Fn(&mut [u8]),
    {
        let mmap = unsafe { self.mmap.get().as_mut().unwrap() };
        let actual_size = self.actual_size.load(Ordering::Relaxed);

        let new_file_size = actual_size + size_inc;
        if mmap.len() < new_file_size {
            return Err(Error::MmapTooSmall);
        }

        let result = {
            self.file
                .set_len(new_file_size as u64)
                .map_err(Error::Write)?;

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
        let mmap = unsafe { self.mmap.get().as_ref().unwrap() };
        let actual_size = self.actual_size.load(Ordering::Relaxed);

        f(&mmap[0..actual_size])
    }
}

unsafe impl Sync for Appender {}

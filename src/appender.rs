//! Appenders are mmap'ed files intended for append-only use.

use crate::{growable_mmap::GrowableMmap, Error};
use std::{
    cell::UnsafeCell,
    fs::OpenOptions,
    marker::Sync,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

pub(crate) struct Appender {
    // This is used to trick the compiler so that we have parallel reads and
    // writes.
    mmap: UnsafeCell<GrowableMmap>,
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
    pub fn new(path: Option<PathBuf>) -> Result<Self, Error> {
        let (file, actual_size) = if let Some(path) = path {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)
                .map_err(|err| Error::FileOpen(path.clone(), err))?;

            let actual_size = file
                .metadata()
                .map_err(|err| Error::FileOpen(path.clone(), err))?
                .len() as usize;

            (Some(file), actual_size)
        } else {
            (None, 0)
        };

        let mmap = UnsafeCell::new(GrowableMmap::new(file)?);
        let actual_size = AtomicUsize::from(actual_size);

        Ok(Self { mmap, actual_size })
    }

    /// Append data to the file. The mutable pointer to the new data location is
    /// given to `f` which should write the data. This function will block if
    /// another write is in progress.
    pub fn append<F>(&self, size_inc: usize, f: F) -> Result<(), Error>
    where
        F: Fn(&mut [u8]),
    {
        if size_inc == 0 {
            return Ok(());
        }

        let mmap = unsafe { self.mmap.get().as_mut().unwrap() };
        let actual_size = self.actual_size.load(Ordering::SeqCst);

        let new_file_size = actual_size + size_inc;

        mmap.grow_and_apply(size_inc, f)?;
        self.actual_size.store(new_file_size, Ordering::SeqCst);

        Ok(())
    }

    /// The whole data buffer is given to `f` which should return the data back
    /// or return None if something went wrong.
    pub fn get_data<F, U>(&self, offset: usize, f: F) -> Option<U>
    where
        F: Fn(&[u8]) -> Option<U>,
    {
        let mmap = unsafe { self.mmap.get().as_ref().unwrap() };
        mmap.get_ref_and_apply(offset, f)
    }

    pub fn size(&self) -> usize {
        self.actual_size.load(Ordering::SeqCst)
    }
}

unsafe impl Sync for Appender {}

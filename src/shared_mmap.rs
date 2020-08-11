use memmap::MmapMut;
use std::{
    io,
    ops::{Bound::*, RangeBounds},
    slice,
    sync::Arc,
};

/// A structure that implements a view into memory mapping.
#[derive(Clone)]
pub struct SharedMmap {
    mmap: Arc<MmapMut>,
    len: usize,
    slice: *mut u8,
}

impl SharedMmap {
    pub(crate) fn new(mut mmap: MmapMut) -> SharedMmap {
        let len = mmap.len();
        let slice = mmap.as_mut_ptr();
        SharedMmap {
            mmap: Arc::new(mmap),
            len,
            slice,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn flush(&self) -> Result<(), io::Error> {
        self.mmap.flush()
    }

    /// Get a sub-view. It will point to the same memory mapping as the parent
    /// mapping.
    pub fn slice(&self, bounds: impl RangeBounds<usize>) -> Option<SharedMmap> {
        let start = match bounds.start_bound() {
            Included(start) => *start,
            Excluded(start) => start + 1,
            Unbounded => 0,
        };

        let end = match bounds.end_bound() {
            Included(end) => *end,
            Excluded(end) => end - 1,
            Unbounded => self.len - 1,
        };
        let end = std::cmp::min(end, self.len - 1);

        let len = if start <= end { end - start + 1 } else { 0 };

        let slice = unsafe { self.slice.add(start) };

        Some(SharedMmap {
            mmap: self.mmap.clone(),
            len,
            slice,
        })
    }

    fn get_ref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.slice, self.len) }
    }

    fn get_mut(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.slice, self.len) }
    }
}

unsafe impl Send for SharedMmap {}

impl AsRef<[u8]> for SharedMmap {
    fn as_ref(&self) -> &[u8] {
        self.get_ref()
    }
}

impl AsMut<[u8]> for SharedMmap {
    fn as_mut(&mut self) -> &mut [u8] {
        self.get_mut()
    }
}

use memmap::MmapMut;
use std::{
    ops::{Bound::*, RangeBounds},
    slice,
    sync::Arc,
};

/// A structure that implements a view into memory mapping.
#[derive(Debug, Clone)]
pub struct SharedMmap {
    mmap: Arc<MmapMut>,
    len: usize,
    slice: *const u8,
}

impl SharedMmap {
    pub(crate) fn new(mmap: MmapMut) -> SharedMmap {
        let len = mmap.len();
        let slice = mmap.as_ptr();
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

    /// Get a sub-view. It will point to the same memory mapping as the parent
    /// mapping.
    pub fn slice(&self, bounds: impl RangeBounds<usize>) -> SharedMmap {
        if self.len == 0 {
            return SharedMmap {
                len: 0,
                ..self.clone()
            };
        }
        let start = match bounds.start_bound() {
            Included(start) => *start,
            Excluded(start) => start + 1,
            Unbounded => 0,
        };

        if start >= self.len {
            return SharedMmap {
                len: 0,
                ..self.clone()
            };
        }

        let end = match bounds.end_bound() {
            Included(end) => *end,
            Excluded(end) if *end == 0 => {
                return SharedMmap {
                    len: 0,
                    ..self.clone()
                };
            }
            Excluded(end) => end - 1,
            Unbounded => self.len - 1,
        };
        let end = std::cmp::min(end, self.len - 1);

        let len = if start <= end { end - start + 1 } else { 0 };
        let slice = unsafe { self.slice.add(start) };

        SharedMmap {
            mmap: self.mmap.clone(),
            len,
            slice,
        }
    }

    fn get_ref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.slice, self.len) }
    }
}

// Those are safe to implement because the underlying `*const u8` is never
// modified.
unsafe impl Send for SharedMmap {}
unsafe impl Sync for SharedMmap {}

impl AsRef<[u8]> for SharedMmap {
    fn as_ref(&self) -> &[u8] {
        self.get_ref()
    }
}

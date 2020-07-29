use memmap::MmapMut;
use std::{
    io,
    ops::{Bound::*, RangeBounds},
    slice,
    sync::Arc,
};

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

    pub fn flush(&self) -> Result<(), io::Error> {
        self.mmap.flush()
    }

    pub fn slice(&self, bounds: impl RangeBounds<usize>) -> Option<SharedMmap> {
        let start = match bounds.start_bound() {
            Included(start) => {
                if *start >= self.len {
                    *start
                } else {
                    return None;
                }
            }
            Excluded(start) => {
                if start + 1 >= self.len {
                    start + 1
                } else {
                    return None;
                }
            }
            Unbounded => 0,
        };

        let end = match bounds.end_bound() {
            Included(end) => {
                if *end >= self.len {
                    *end
                } else {
                    return None;
                }
            }
            Excluded(end) => {
                if end - 1 >= self.len {
                    end - 1
                } else {
                    return None;
                }
            }
            Unbounded => self.len - 1,
        };

        let len = end - start;

        if len == 0 {
            return None;
        }

        let slice = unsafe { self.slice.offset(start as isize) };

        Some(SharedMmap {
            mmap: self.mmap.clone(),
            len,
            slice,
        })
    }
}

unsafe impl Send for SharedMmap {}

impl AsRef<[u8]> for SharedMmap {
    fn as_ref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.slice, self.len) }
    }
}

impl AsMut<[u8]> for SharedMmap {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.slice, self.len) }
    }
}

// TODO implement `SliceIndex` when stabilized

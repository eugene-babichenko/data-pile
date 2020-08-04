use crate::flatfile::FlatFile;
use std::{mem::size_of, sync::Arc};

/// This structure allows to iterate over records in the order they were added
/// to this database. Note that this structure provides only the `Iterator`-like
/// interface, because `Iterator` items cannot have associated lifetimes.
pub struct SeqNoIter {
    data: Arc<FlatFile>,
    offset: usize,
}

impl SeqNoIter {
    pub(crate) fn new(data: Arc<FlatFile>, offset: usize) -> Self {
        Self { data, offset }
    }

    pub fn next(&mut self) -> Option<&[u8]> {
        let item = self.data.get_record_at_offset(self.offset)?;
        self.offset += item.len() + size_of::<u64>();
        Some(item)
    }
}

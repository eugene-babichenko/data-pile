use crate::{flatfile::FlatFile, SharedMmap};
use std::{mem::size_of, sync::Arc};

/// This structure allows to iterate over records in the order they were added
/// to this database.
pub struct SeqNoIter {
    data: Arc<FlatFile>,
    offset: usize,
}

impl SeqNoIter {
    pub(crate) fn new(data: Arc<FlatFile>, offset: usize) -> Self {
        Self { data, offset }
    }

    fn next_impl(&mut self) -> Option<SharedMmap> {
        let item = self.data.get_record_at_offset(self.offset)?;
        self.offset += item.len() + size_of::<u64>();
        Some(item)
    }
}

impl Iterator for SeqNoIter {
    type Item = SharedMmap;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_impl()
    }
}

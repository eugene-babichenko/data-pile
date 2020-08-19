use crate::{flatfile::FlatFile, seqno::SeqNoIndex, SharedMmap};
use std::sync::Arc;

/// This structure allows to iterate over records in the order they were added
/// to this database.
pub struct SeqNoIter {
    data: Arc<FlatFile>,
    index: Arc<SeqNoIndex>,
    seqno: usize,
}

impl SeqNoIter {
    pub(crate) fn new(data: Arc<FlatFile>, index: Arc<SeqNoIndex>, seqno: usize) -> Self {
        Self { data, index, seqno }
    }

    fn next_impl(&mut self) -> Option<SharedMmap> {
        let offset = self.index.get_pointer_to_value(self.seqno)? as usize;
        let next_offset = self
            .index
            .get_pointer_to_value(self.seqno + 1)
            .map(|value| value as usize)
            .unwrap_or_else(|| self.data.len());
        let length = next_offset - offset;
        let item = self.data.get_record_at_offset(offset, length)?;
        self.seqno += 1;
        Some(item)
    }
}

impl Iterator for SeqNoIter {
    type Item = SharedMmap;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_impl()
    }
}

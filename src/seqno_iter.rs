use crate::{flatfile::FlatFile, Record, RecordSerializer};
use std::sync::Arc;

/// This structure allows to iterate over records in the order they were added
/// to this database. Note that this structure provides only the `Iterator`-like
/// interface, because `Iterator` items cannot have associated lifetimes.
pub struct SeqNoIter<R: RecordSerializer> {
    data: Arc<FlatFile>,
    serializer: R,
    offset: usize,
}

impl<'a, R: RecordSerializer> SeqNoIter<R> {
    pub(crate) fn new(data: Arc<FlatFile>, serializer: R, offset: usize) -> Self {
        Self {
            data,
            serializer,
            offset,
        }
    }

    pub fn next(&mut self) -> Option<Record> {
        let item = self
            .data
            .get_record_at_offset(&self.serializer, self.offset)?;
        self.offset += self.serializer.size(&item);
        Some(item)
    }
}

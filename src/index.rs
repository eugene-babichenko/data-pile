use crate::{flatfile::FlatFile, RecordSerializer, SeqNoIter};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

#[derive(Clone)]
pub(crate) struct Index {
    mapping: Arc<RwLock<BTreeMap<Box<[u8]>, usize>>>,
}

impl Index {
    pub fn new<R: RecordSerializer + Clone>(data: Arc<FlatFile>, serializer: R) -> Self {
        let mut iter = SeqNoIter::new(data.clone(), serializer.clone(), 0);
        let mut offset = 0;
        let mut mapping = BTreeMap::new();
        while let Some(record) = iter.next() {
            let key = record.key().to_owned().into_boxed_slice();
            mapping.insert(key, offset);
            offset += serializer.size(&record);
        }

        let mapping = Arc::new(RwLock::new(mapping));

        Self { mapping }
    }

    pub fn put(&self, key: &[u8], offset: usize) {
        let mut guard = self.mapping.write().unwrap();
        guard.insert(key.to_owned().into_boxed_slice(), offset);
    }

    pub fn get(&self, key: &[u8]) -> Option<usize> {
        let guard = self.mapping.read().unwrap();
        guard.get(key).map(|offset| *offset)
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        let guard = self.mapping.read().unwrap();
        guard.contains_key(key)
    }
}

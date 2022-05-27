pub struct SingleMmapIndex {
    pub internal_bounds: Vec<usize>,
    start: usize,
}

impl SingleMmapIndex {
    pub fn new(start: usize) -> Self {
        Self {
            internal_bounds: Vec::new(),
            start,
        }
    }

    pub fn last_global_index(&self) -> usize {
        self.start + self.internal_bounds.last().copied().unwrap_or(0usize)
    }

    pub fn current_mmap_size(&self) -> usize {
        self.internal_bounds.last().copied().unwrap_or(0usize)
    }

    pub fn append(&mut self, end: usize) {
        let start = self.internal_bounds.last().copied().unwrap_or(0);
        assert!(start < end);

        if self.internal_bounds.is_empty() {
            self.internal_bounds.push(0);
        }

        self.internal_bounds.push(end);
    }

    pub fn is_empty(&self) -> bool {
        self.last_global_index() == 0
    }

    pub fn find(&self, address: usize) -> Option<IndexDescriptor> {
        if address < self.start {
            return None;
        }
        match self.internal_bounds.binary_search(&(address - self.start)) {
            Ok(mmap_offset_position) => {
                if address - self.start == self.internal_bounds.last().copied().unwrap_or(0usize) {
                    return None;
                }
                let mmap_offset = self.internal_bounds[mmap_offset_position];
                let len = self
                    .internal_bounds
                    .get(mmap_offset_position + 1)
                    .unwrap_or(&mmap_offset)
                    - mmap_offset;
                Some(IndexDescriptor {
                    mmap_number: 0,
                    mmap_offset,
                    len,
                })
            }
            Err(position) => {
                let upper = self.internal_bounds[position];
                let mmap_offset = address - self.start;
                Some(IndexDescriptor {
                    mmap_number: 0,
                    mmap_offset,
                    len: upper - mmap_offset,
                })
            }
        }
    }
}

pub struct IndexOnMmaps {
    mmaps: Vec<SingleMmapIndex>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct IndexDescriptor {
    pub mmap_number: usize,
    pub mmap_offset: usize,
    pub len: usize,
}

impl IndexOnMmaps {
    pub fn new() -> Self {
        Self { mmaps: Vec::new() }
    }

    pub fn add_mmap(&mut self, mmap_index: SingleMmapIndex) {
        if mmap_index.is_empty() {
            return;
        }
        let current_end = self
            .mmaps
            .last()
            .map(|last| last.last_global_index())
            .unwrap_or(0);
        assert_eq!(current_end, mmap_index.start);

        self.mmaps.push(mmap_index);
    }

    pub fn find(&self, address: usize) -> Option<IndexDescriptor> {
        let mmap_number = match self
            .mmaps
            .binary_search_by_key(&address, |mmap_index| -> usize {
                if mmap_index.start <= address && address < mmap_index.last_global_index() {
                    address
                } else {
                    mmap_index.start
                }
            }) {
            Ok(position) => position,
            Err(_) => return None,
        };

        let index = self.mmaps[mmap_number].find(address)?;
        Some(IndexDescriptor {
            mmap_number,
            mmap_offset: index.mmap_offset,
            len: index.len,
        })
    }

    pub fn memory_size(&self) -> usize {
        self.mmaps
            .last()
            .map(|mmap_index| mmap_index.last_global_index())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::{IndexDescriptor, IndexOnMmaps};
    use crate::index_on_mmaps::SingleMmapIndex;

    #[test]
    fn index() {
        let data = [vec![34], vec![42, 67], vec![96, 103, 420]];
        let mut index = IndexOnMmaps::new();

        for item in data.iter() {
            let mut single_mmap_index = SingleMmapIndex::new(index.memory_size());
            for sub_item in item {
                single_mmap_index.append(*sub_item - index.memory_size());
            }
            index.add_mmap(single_mmap_index);
        }

        assert_eq!(
            Some(IndexDescriptor {
                len: 34,
                mmap_offset: 0,
                mmap_number: 0,
            }),
            index.find(0)
        );
        assert_eq!(
            Some(IndexDescriptor {
                len: 8,
                mmap_offset: 0,
                mmap_number: 1,
            }),
            index.find(34)
        );
        assert_eq!(
            Some(IndexDescriptor {
                len: 25,
                mmap_offset: 8,
                mmap_number: 1,
            }),
            index.find(42)
        );
        assert_eq!(
            Some(IndexDescriptor {
                len: 29,
                mmap_offset: 0,
                mmap_number: 2,
            }),
            index.find(67)
        );
        assert_eq!(
            Some(IndexDescriptor {
                len: 7,
                mmap_offset: 29,
                mmap_number: 2,
            }),
            index.find(96)
        );
        assert_eq!(None, index.find(420));
        assert_eq!(None, index.find(1000));
    }
}

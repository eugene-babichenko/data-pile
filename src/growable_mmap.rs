use crate::index_on_mmaps::{IndexDescriptor, IndexOnMmaps, SingleMmapIndex};
use crate::{Error, SharedMmap};
use memmap2::{MmapMut, MmapOptions};
use std::cmp::max;
use std::convert::TryInto;
use std::fs::File;
use std::io::Write;
use std::mem::{size_of, swap};
use std::sync::RwLock;

struct StorageHeader {
    mmap: MmapMut,
}

impl StorageHeader {
    pub const HEADER_SIZE: usize = size_of::<usize>() * 2;

    pub fn new(file: &Option<File>) -> Result<StorageHeader, Error> {
        if let Some(file) = &file {
            if file.metadata().map_err(Error::Metadata)?.len() > 0 {
                let mmap = unsafe {
                    MmapOptions::new()
                        .len(StorageHeader::HEADER_SIZE)
                        .map_mut(file)
                }
                .map_err(Error::Mmap)?;
                return Ok(StorageHeader { mmap });
            }
            file.set_len(StorageHeader::HEADER_SIZE as u64)
                .map_err(Error::Extend)?;
            let mmap = unsafe {
                MmapOptions::new()
                    .len(StorageHeader::HEADER_SIZE)
                    .map_mut(file)
            }
            .map_err(Error::Mmap)?;

            let mut header = StorageHeader { mmap };
            header.store_storage_size(0)?;
            header.flush()?;
            return Ok(header);
        }
        let mmap = MmapOptions::new()
            .len(StorageHeader::HEADER_SIZE)
            .map_anon()
            .map_err(Error::Mmap)?;
        Ok(StorageHeader { mmap })
    }

    pub fn load_storage_size(&self) -> Result<usize, Error> {
        let bytes = &self.mmap.as_ref()[..size_of::<usize>()];
        Ok(usize::from_le_bytes(
            bytes.try_into().map_err(|_| Error::ReadHeader)?,
        ))
    }

    pub fn store_storage_size(&mut self, new_size: usize) -> Result<(), Error> {
        let mut mmap = &mut self.mmap.as_mut()[0..size_of::<usize>()];
        mmap.write_all(new_size.to_le_bytes().as_ref())
            .map_err(Error::UpdateHeader)?;
        self.flush()
    }

    fn flush(&self) -> Result<(), Error> {
        self.mmap.flush().map_err(Error::Flush)
    }
}

struct ActiveMmap {
    len: usize,
    mmap: MmapMut,
    bounds: SingleMmapIndex,
}

struct InactiveMmaps {
    index: IndexOnMmaps,
    maps: Vec<SharedMmap>,
}

struct Storage {
    header: StorageHeader,
    inactive_mmaps: InactiveMmaps,
    active_map: Option<ActiveMmap>,
}

/// the struct has an active mutable mmap and inactive tail
/// if we have enough space we add records to the active mmap
/// if not we slice the active mmap to the actual end of writes and put it to inactive mmaps
/// then we create a new mmap with 2x size from previous
/// if 2x is not enough we create an mmap with size of the data
///
/// TODO: make inactive mmaps locked separately from active mmap / replace the vector with thread-safe solution
pub struct GrowableMmap {
    storage: RwLock<Storage>,
    file: Option<File>,
}

impl GrowableMmap {
    pub fn new(file: Option<File>) -> Result<Self, Error> {
        let mut index = IndexOnMmaps::new();
        let mut maps = vec![];

        let header = StorageHeader::new(&file)?;
        let current_storage_size = header.load_storage_size()?;

        if let Some(file) = &file {
            if file.metadata().map_err(Error::Metadata)?.len() > StorageHeader::HEADER_SIZE as u64 {
                let mmap = SharedMmap::new(
                    unsafe {
                        MmapOptions::new()
                            .offset(StorageHeader::HEADER_SIZE as u64)
                            .len(current_storage_size)
                            .map(file)
                    }
                    .map_err(Error::Mmap)?,
                );
                let mut single_mmap_index = SingleMmapIndex::new(0usize);
                single_mmap_index.append(mmap.len());
                index.add_mmap(single_mmap_index);
                maps.push(mmap);
            }
        }

        let growable_mmap = GrowableMmap {
            storage: RwLock::new(Storage {
                header,
                inactive_mmaps: InactiveMmaps { index, maps },
                active_map: None,
            }),
            file,
        };

        Ok(growable_mmap)
    }

    pub fn data_size(&self) -> Result<usize, Error> {
        self.storage
            .read()
            .map_err(|_| Error::StorageLock)?
            .header
            .load_storage_size()
    }

    pub fn grow_and_apply<F>(&self, add: usize, f: F) -> Result<(), Error>
    where
        F: Fn(&mut [u8]) -> Result<(), Error>,
    {
        assert_ne!(add, 0, "no grow in file size");

        let mut storage_guard = self
            .storage
            .write()
            .map_err(|_| -> Error { Error::StorageLock })?;
        let start_write_from = match &mut storage_guard.active_map {
            None => {
                let new_mmap_size = self.get_new_mmap_size(add, None);
                // header + inactive size
                let already_mapped =
                    StorageHeader::HEADER_SIZE + storage_guard.inactive_mmaps.index.memory_size();

                // create mmap and flush
                let new_mmap = self.create_mmap(new_mmap_size, already_mapped)?;
                new_mmap.flush().map_err(Error::Flush)?;

                // create index on active mmap
                let mut single_mmap_index =
                    SingleMmapIndex::new(already_mapped - StorageHeader::HEADER_SIZE);
                single_mmap_index.append(add);

                storage_guard.active_map = Some(ActiveMmap {
                    len: new_mmap_size,
                    mmap: new_mmap,
                    bounds: single_mmap_index,
                });

                0usize
            }
            Some(active_mmap) => {
                let current_mmap_end = active_mmap.bounds.current_mmap_size();

                // if we have enough space use active mmap
                if current_mmap_end + add < active_mmap.len {
                    active_mmap.bounds.append(current_mmap_end + add);
                    current_mmap_end
                } else {
                    let new_mmap_size = self.get_new_mmap_size(add, Some(active_mmap.len));
                    // offset is header + inactive part + current active part
                    let already_mapped =
                        StorageHeader::HEADER_SIZE + active_mmap.bounds.last_global_index();

                    let mut new_mmap = self.create_mmap(new_mmap_size, already_mapped)?;
                    new_mmap.flush().map_err(Error::Flush)?;

                    // replace active mmap with new mmap
                    swap(&mut new_mmap, &mut active_mmap.mmap);
                    active_mmap.len = new_mmap_size;

                    let mut new_bounds =
                        SingleMmapIndex::new(already_mapped - StorageHeader::HEADER_SIZE);
                    new_bounds.append(add);
                    swap(&mut new_bounds, &mut active_mmap.bounds);

                    // add old replaced active mmap to inactive mmaps
                    storage_guard.inactive_mmaps.index.add_mmap(new_bounds);
                    storage_guard.inactive_mmaps.maps.push(
                        SharedMmap::new(new_mmap.make_read_only().map_err(Error::Protect)?)
                            .slice(..current_mmap_end),
                    );

                    0usize
                }
            }
        };

        match storage_guard.active_map.as_mut() {
            None => Err(Error::DataFileDamaged),
            Some(active_mmap) => {
                f(&mut active_mmap.mmap.as_mut()[start_write_from..])?;
                active_mmap.mmap.flush().map_err(Error::Flush)?;

                let current_size = storage_guard.header.load_storage_size()?;
                storage_guard
                    .header
                    .store_storage_size(current_size + add)?;

                Ok(())
            }
        }
    }

    pub fn get_ref_and_apply<F, U>(&self, address: usize, f: F) -> Option<U>
    where
        F: Fn(&[u8]) -> Option<U>,
    {
        let storage_guard = if let Ok(storage) = self.storage.read() {
            storage
        } else {
            return None;
        };

        if address < storage_guard.inactive_mmaps.index.memory_size() {
            let IndexDescriptor {
                mmap_number,
                mmap_offset,
                len,
            } = storage_guard.inactive_mmaps.index.find(address)?;

            return f(storage_guard.inactive_mmaps.maps[mmap_number]
                .slice(mmap_offset..mmap_offset + len)
                .as_ref());
        }

        match storage_guard.active_map.as_ref() {
            None => None,
            Some(active_mmap) => {
                let IndexDescriptor {
                    mmap_number: _mmap_number,
                    mmap_offset,
                    len,
                } = active_mmap.bounds.find(address)?;

                f(&active_mmap.mmap.as_ref()[mmap_offset..mmap_offset + len])
            }
        }
    }

    fn get_new_mmap_size(&self, add: usize, active_mmap_size: Option<usize>) -> usize {
        match self.file {
            None => add,
            Some(_) => {
                let active_mmap = active_mmap_size.unwrap_or(2048);
                max(add, active_mmap * 2)
            }
        }
    }

    fn create_mmap(&self, new_mmap_size: usize, offset: usize) -> Result<MmapMut, Error> {
        if let Some(file) = &self.file {
            file.set_len((offset + new_mmap_size) as u64)
                .map_err(Error::Extend)?;
            unsafe {
                MmapOptions::new()
                    .len(new_mmap_size)
                    .offset(offset as u64)
                    .map_mut(file)
            }
            .map_err(Error::Mmap)
        } else {
            MmapOptions::new()
                .len(new_mmap_size)
                .map_anon()
                .map_err(Error::Mmap)
        }
    }
}

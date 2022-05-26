use crate::index_on_mmaps::{IndexDescriptor, IndexOnMmaps, SingleMmapIndex};
use crate::{Error, SharedMmap};
use memmap2::{MmapMut, MmapOptions};
use std::cmp::max;
use std::fs::File;
use std::mem::swap;
use std::sync::RwLock;

struct ActiveMmap {
    len: usize,
    mmap: MmapMut,
    bounds: SingleMmapIndex,
}

struct InactiveMmaps {
    index: IndexOnMmaps,
    maps: Vec<SharedMmap>,
}

pub struct GrowableMmap {
    inactive_mmaps: RwLock<InactiveMmaps>,
    active_map: RwLock<Option<ActiveMmap>>,
    file: Option<File>,
}

impl GrowableMmap {
    pub fn new(file: Option<File>) -> Result<Self, Error> {
        let mut index = IndexOnMmaps::new();
        let mut maps = vec![];

        if let Some(file) = &file {
            if file.metadata().map_err(Error::Metadata)?.len() > 0 {
                let mmap =
                    SharedMmap::new(unsafe { MmapOptions::new().map(file) }.map_err(Error::Mmap)?);
                let mut single_mmap_index = SingleMmapIndex::new(0usize);
                single_mmap_index.append(mmap.len());
                index.add_mmap(single_mmap_index);
                maps.push(mmap);
            }
        }

        let growable_mmap = GrowableMmap {
            inactive_mmaps: RwLock::new(InactiveMmaps { index, maps }),
            active_map: RwLock::new(None),
            file,
        };

        Ok(growable_mmap)
    }

    pub fn grow_and_apply<F>(&self, add: usize, f: F) -> Result<(), Error>
    where
        F: Fn(&mut [u8]),
    {
        assert_ne!(add, 0, "no grow in file size");

        let mut active_map_guard = self
            .active_map
            .write()
            .map_err(|_| -> Error { Error::Lock })?;
        let start_write_from = match active_map_guard.as_mut() {
            None => {
                let new_mmap_size = self.get_new_mmap_size(add, None);
                let new_mmap = self.create_mmap(new_mmap_size, 0usize)?;
                new_mmap.flush().map_err(Error::Flush)?;

                let mut single_mmap_index = SingleMmapIndex::new(0usize);
                single_mmap_index.append(add);

                *active_map_guard = Some(ActiveMmap {
                    len: new_mmap_size,
                    mmap: new_mmap,
                    bounds: single_mmap_index,
                });

                0usize
            }
            Some(active_mmap) => {
                let current_mmap_end = active_mmap.bounds.current_mmap_size();

                if current_mmap_end + add < active_mmap.len {
                    active_mmap.bounds.append(current_mmap_end + add);
                    current_mmap_end
                } else {
                    let mut inactive_mmaps_guard = self
                        .inactive_mmaps
                        .write()
                        .map_err(|_| -> Error { Error::Lock })?;

                    let new_mmap_size = self.get_new_mmap_size(add, Some(active_mmap.len));
                    let mut new_mmap =
                        self.create_mmap(new_mmap_size, active_mmap.bounds.last_global_index())?;
                    new_mmap.flush().map_err(Error::Flush)?;

                    swap(&mut new_mmap, &mut active_mmap.mmap);

                    inactive_mmaps_guard.maps.push(
                        SharedMmap::new(new_mmap.make_read_only().map_err(Error::Protect)?)
                            .slice(..current_mmap_end),
                    );

                    active_mmap.len = new_mmap_size;

                    let mut new_bounds = SingleMmapIndex::new(active_mmap.bounds.last_global_index());
                    new_bounds.append(add);
                    swap(&mut new_bounds, &mut active_mmap.bounds);
                    inactive_mmaps_guard.index.add_mmap(new_bounds);

                    0usize
                }
            }
        };

        match active_map_guard.as_mut() {
            None => Err(Error::InvalidState),
            Some(active_mmap) => {
                f(&mut active_mmap.mmap.as_mut()[start_write_from..]);
                active_mmap.mmap.flush().map_err(Error::Flush)?;
                Ok(())
            }
        }
    }

    pub fn get_new_mmap_size(&self, add: usize, active_mmap: Option<usize>) -> usize {
        match self.file {
            None => add,
            Some(_) => {
                let active_mmap = match active_mmap {
                    None => 128,
                    Some(len) => len * 2,
                };
                max(add, active_mmap * 2)
            }
        }
    }

    pub fn create_mmap(&self, new_mmap_size: usize, offset: usize) -> Result<MmapMut, Error> {
        if let Some(file) = &self.file {
            file.set_len((offset + new_mmap_size) as u64).map_err(Error::Extend)?;
            unsafe {
                MmapOptions::new()
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

    pub fn get_ref_and_apply<F, U>(&self, address: usize, f: F) -> Option<U>
    where
        F: Fn(&[u8]) -> Option<U>,
    {
        let inactive_mmaps_guard = if let Ok(inactive_mmaps_guard) = self.inactive_mmaps.read() {
            inactive_mmaps_guard
        } else {
            return None;
        };

        if address < inactive_mmaps_guard.index.memory_size() {
            let IndexDescriptor {
                mmap_number,
                mmap_offset,
                len,
            } = inactive_mmaps_guard.index.find(address)?;

            return f(inactive_mmaps_guard.maps[mmap_number]
                .slice(mmap_offset..mmap_offset + len)
                .as_ref());
        }

        let active_mmap_guard = if let Ok(active_mmap_guard) = self.active_map.read() {
            active_mmap_guard
        } else {
            return None;
        };

        match active_mmap_guard.as_ref() {
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
}

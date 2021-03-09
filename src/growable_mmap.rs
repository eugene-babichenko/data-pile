use crate::{
    page_index::{PageDescriptor, PageIndex},
    Error, SharedMmap,
};
use memmap2::{MmapMut, MmapOptions};
use std::fs::File;

pub struct GrowableMmap {
    index: PageIndex,
    maps: Vec<SharedMmap>,
    file: Option<File>,
}

impl GrowableMmap {
    pub fn new(file: Option<File>) -> Result<Self, Error> {
        let index = PageIndex::new();
        let maps = vec![];

        let mut growable_mmap = GrowableMmap { index, maps, file };

        if let Some(file) = &growable_mmap.file {
            if file.metadata().map_err(Error::Metadata)?.len() > 0 {
                let mmap =
                    SharedMmap::new(unsafe { MmapOptions::new().map(&file) }.map_err(Error::Mmap)?);
                growable_mmap.index.add_page(mmap.len());
                growable_mmap.maps.push(mmap);
            }
        }

        Ok(growable_mmap)
    }

    pub fn grow(&self, add: usize) -> Result<MmapMut, Error> {
        assert_ne!(add, 0, "no grow in file size");

        if let Some(file) = &self.file {
            let current_len = self.index.memory_size();

            let new_len = current_len + add;

            file.set_len(new_len as u64).map_err(Error::Extend)?;

            return unsafe { MmapOptions::new().offset(current_len as u64).map_mut(file) }
                .map_err(Error::Mmap);
        }

        MmapOptions::new().len(add).map_anon().map_err(Error::Mmap)
    }

    pub fn append_page(&mut self, page: MmapMut) -> Result<(), Error> {
        let current_len = self.index.memory_size();
        let new_len = current_len + page.len();

        page.flush().map_err(Error::Flush)?;

        let page = SharedMmap::new(page.make_read_only().map_err(Error::Protect)?);
        self.maps.push(page);
        self.index.add_page(new_len);

        Ok(())
    }

    pub fn get_ref(&self, address: usize) -> Option<SharedMmap> {
        let PageDescriptor {
            len,
            offset,
            number,
        } = self.index.find(address)?;
        Some(self.maps[number].slice((address - offset)..len))
    }
}

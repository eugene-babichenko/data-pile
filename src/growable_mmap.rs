use crate::{
    page_index::{PageDescriptor, PageIndex},
    Error, SharedMmap,
};
use memmap::{MmapMut, MmapOptions};
use std::fs::File;

pub struct GrowableMmap {
    index: PageIndex,
    maps: Vec<SharedMmap>,
    file: File,
}

impl GrowableMmap {
    pub fn new(file: File) -> Result<Self, Error> {
        let index = PageIndex::new();
        let maps = vec![];

        let mut growable_mmap = GrowableMmap { index, maps, file };

        if growable_mmap
            .file
            .metadata()
            .map_err(Error::Metadata)?
            .len()
            > 0
        {
            let mmap = SharedMmap::new(
                unsafe { MmapOptions::new().map_mut(&growable_mmap.file) }.map_err(Error::Mmap)?,
            );
            growable_mmap.index.add_page(mmap.len());
            growable_mmap.maps.push(mmap);
        }

        Ok(growable_mmap)
    }

    pub fn grow(&self, add: usize) -> Result<MmapMut, Error> {
        assert_ne!(add, 0, "no grow in file size");

        let current_len = self.index.memory_size();

        let new_len = current_len + add;
        self.file.set_len(new_len as u64).map_err(Error::Extend)?;

        unsafe {
            MmapOptions::new()
                .offset(current_len as u64)
                .map_mut(&self.file)
        }
        .map_err(Error::Mmap)
    }

    pub fn append_page(&mut self, page: MmapMut) -> Result<(), Error> {
        let current_len = self.index.memory_size();
        let new_len = current_len + page.len();

        page.flush().map_err(Error::Flush)?;

        let page = SharedMmap::new(page);
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

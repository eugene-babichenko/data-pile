use crate::{
    page_index::{PageDescriptor, PageIndex},
    Error,
};
use memmap::{MmapMut, MmapOptions};
use std::{fs::File, sync::RwLock};

pub struct GrowableMmap {
    index: RwLock<PageIndex>,
    maps: Vec<MmapMut>,
    file: File,
}

impl GrowableMmap {
    pub fn new(file: File) -> Result<Self, Error> {
        let index = RwLock::new(PageIndex::new());
        let maps = vec![];

        let mut growable_mmap = GrowableMmap { index, maps, file };

        if growable_mmap
            .file
            .metadata()
            .map_err(Error::Metadata)?
            .len()
            > 0
        {
            let mmap =
                unsafe { MmapOptions::new().map_mut(&growable_mmap.file) }.map_err(Error::Mmap)?;
            growable_mmap.index.write().unwrap().add_page(0, mmap.len());
            growable_mmap.maps.push(mmap);
        }

        Ok(growable_mmap)
    }

    pub fn grow(&mut self, starting_point: usize, add: usize) -> Result<(), Error> {
        let mut index = self.index.write().unwrap();
        if index.is_empty() {
            assert_eq!(
                0, starting_point,
                "should not specify non-zero offset for a zero-sized file"
            );
            assert_ne!(0, add, "no increase in file size");

            self.file.set_len(add as u64).map_err(Error::Extend)?;
            index.add_page(0, add);
            self.maps
                .push(unsafe { MmapOptions::new().map_mut(&self.file) }.map_err(Error::Mmap)?);

            return Ok(());
        }

        let current_len = index.memory_size();
        assert!(
            starting_point < current_len,
            "cannot start ({}) outside of the file boundary ({})",
            starting_point,
            current_len
        );

        let new_len = 1 + starting_point + add;
        assert!(new_len > current_len, "no increase in file size");

        self.file.set_len(new_len as u64).map_err(Error::Extend)?;

        let mmap = unsafe {
            MmapOptions::new()
                .offset((starting_point + 1) as u64)
                .map_mut(&self.file)
        }
        .map_err(Error::Mmap)?;
        self.maps.push(mmap);
        index.add_page(starting_point + 1, new_len);

        Ok(())
    }

    pub fn flush_last(&self) -> Result<(), Error> {
        if let Some(page) = self.maps.last() {
            page.flush().map_err(Error::Flush)?;
        }

        Ok(())
    }

    pub fn get_mut_last(&mut self) -> Option<&mut [u8]> {
        self.maps.last_mut().map(|page| page.as_mut())
    }

    pub fn get_ref(&self, address: usize) -> Option<&[u8]> {
        let PageDescriptor {
            len,
            offset,
            number,
        } = self.index.read().unwrap().find(address)?;
        Some(&self.maps[number][(address - offset)..len])
    }

    pub fn snapshot(&self, end: usize) -> Result<impl AsRef<[u8]>, Error> {
        unsafe { MmapOptions::new().len(end).map(&self.file) }.map_err(Error::Mmap)
    }
}

use crate::Error;
use memmap::{MmapMut, MmapOptions};
use std::{fs::File, ops::Range};

pub struct GrowableMmap {
    ranges: Vec<Range<usize>>,
    maps: Vec<MmapMut>,
    file: File,
}

impl GrowableMmap {
    pub fn new(file: File) -> Result<Self, Error> {
        if file.metadata().map_err(Error::Metadata)?.len() == 0 {
            return Ok(Self {
                ranges: vec![],
                maps: vec![],
                file,
            });
        }

        let mmap = unsafe { MmapOptions::new().map_mut(&file) }.map_err(Error::Mmap)?;
        Ok(GrowableMmap {
            ranges: vec![0..mmap.len()],
            maps: vec![mmap],
            file,
        })
    }

    pub fn grow(&mut self, starting_point: usize, add: usize) -> Result<(), Error> {
        if self.ranges.is_empty() {
            assert_eq!(
                0, starting_point,
                "should not specify non-zero offset for a zero-sized file"
            );
            assert_ne!(0, add, "no increase in file size");

            self.file.set_len(add as u64).map_err(Error::Extend)?;
            self.ranges.push(0..add);
            self.maps
                .push(unsafe { MmapOptions::new().map_mut(&self.file) }.map_err(Error::Mmap)?);

            return Ok(());
        }

        let current_len = self.ranges.last().unwrap().end;
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
        self.ranges.last_mut().unwrap().end = starting_point + 1;
        self.ranges.push((starting_point + 1)..new_len);

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
        let (position, bounds) = self
            .ranges
            .iter()
            .enumerate()
            .find(|(_, bounds)| bounds.contains(&address))?;

        let start = address - bounds.start;
        let end = bounds.end - bounds.start;

        Some(&self.maps[position][start..end])
    }
}

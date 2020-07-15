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

    pub fn len(&self) -> usize {
        self.ranges.last().map(|range| range.end).unwrap_or(0)
    }

    pub fn get_ref(&self, address: usize) -> Option<&[u8]> {
        let (position, begin, end) = self.get_position(address)?;
        let mmap = self.maps[position].as_ref();
        Some(&mmap[begin..end])
    }

    pub fn get_mut(&mut self, address: usize) -> Option<&mut [u8]> {
        let (position, begin, end) = self.get_position(address)?;
        let mmap = self.maps[position].as_mut();
        Some(&mut mmap[begin..end])
    }

    fn get_position(&self, address: usize) -> Option<(usize, usize, usize)> {
        let (position, boundaries) = self
            .ranges
            .iter()
            .enumerate()
            .find(|(_, boundaries)| boundaries.contains(&address))?;

        let segment_beginning = boundaries.start;
        let begin = address - segment_beginning;
        let end = boundaries.end - boundaries.start;

        Some((position, begin, end))
    }
}

#[cfg(test)]
mod tests {
    use super::GrowableMmap;

    #[test]
    pub fn grow() {
        let tmp = tempfile::tempfile().unwrap();

        let mut mmap = GrowableMmap::new(tmp).unwrap();
        assert!(mmap.get_ref(0).is_none());

        mmap.grow(0, 10).unwrap();

        let page = mmap.get_mut(0).unwrap();
        page.copy_from_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

        let page = mmap.get_ref(4).unwrap();
        assert_eq!(&[4, 5, 6, 7, 8, 9], page);

        assert!(mmap.get_ref(10).is_none());

        mmap.grow(5, 10).unwrap();

        let page = mmap.get_ref(6).unwrap();
        assert_eq!(&[6, 7, 8, 9, 0, 0, 0, 0, 0, 0], page);

        let page = mmap.get_mut(8).unwrap();
        assert_eq!(&[8, 9, 0, 0, 0, 0, 0, 0], page);

        page.copy_from_slice(&[1, 1, 1, 1, 1, 1, 1, 1]);

        let page = mmap.get_mut(0).unwrap();
        assert_eq!(&[0, 1, 2, 3, 4, 5], page);

        assert!(mmap.get_ref(16).is_none());
    }
}

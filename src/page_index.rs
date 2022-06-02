pub struct PageIndex {
    bounds: Vec<usize>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct PageDescriptor {
    pub len: usize,
    pub offset: usize,
    pub number: usize,
}

impl PageIndex {
    pub fn new() -> Self {
        Self { bounds: Vec::new() }
    }

    pub fn add_page(&mut self, end: usize) {
        let start = self.bounds.last().copied().unwrap_or(0);
        assert!(start < end);

        if self.bounds.is_empty() {
            self.bounds.push(0);
        }

        self.bounds.push(end);
    }

    pub fn find(&self, address: usize) -> Option<PageDescriptor> {
        find_impl(&self.bounds, 0, address)
    }

    pub fn memory_size(&self) -> usize {
        self.bounds.last().copied().unwrap_or(0)
    }
}

fn find_impl(bounds: &[usize], offset: usize, address: usize) -> Option<PageDescriptor> {
    if bounds.len() < 2 {
        return None;
    }

    let end_position = bounds.len() / 2;
    let start_position = end_position - 1;

    if bounds[start_position] <= address && address < bounds[end_position] {
        Some(PageDescriptor {
            len: bounds[end_position] - bounds[start_position],
            offset: bounds[start_position],
            number: offset + start_position,
        })
    } else if address < bounds[start_position] {
        find_impl(&bounds[..end_position], offset, address)
    } else {
        find_impl(&bounds[end_position..], offset + end_position, address)
    }
}

#[cfg(test)]
mod tests {
    use super::{PageDescriptor, PageIndex};

    #[test]
    fn index() {
        let data = [34, 42, 67, 96, 103, 420];
        let mut index = PageIndex::new();

        for item in data.iter().copied() {
            index.add_page(item);
        }

        assert_eq!(
            Some(PageDescriptor {
                len: 34,
                offset: 0,
                number: 0,
            }),
            index.find(10)
        );
        assert_eq!(
            Some(PageDescriptor {
                len: 29,
                offset: 67,
                number: 3,
            }),
            index.find(80)
        );
        assert_eq!(
            Some(PageDescriptor {
                len: 7,
                offset: 96,
                number: 4,
            }),
            index.find(102)
        );
        assert_eq!(
            Some(PageDescriptor {
                len: 317,
                offset: 103,
                number: 5,
            }),
            index.find(103)
        );
        assert_eq!(
            Some(PageDescriptor {
                len: 317,
                offset: 103,
                number: 5,
            }),
            index.find(200)
        );
        assert_eq!(None, index.find(420));
        assert_eq!(None, index.find(1000));
    }
}

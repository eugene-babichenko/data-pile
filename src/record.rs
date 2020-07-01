/// A database record.
pub struct Record<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> Record<'a> {
    pub fn new(key: &'a [u8], value: &'a [u8]) -> Self {
        Self { key, value }
    }

    pub fn key(&self) -> &'a [u8] {
        self.key
    }

    pub fn value(&self) -> &'a [u8] {
        self.value
    }
}

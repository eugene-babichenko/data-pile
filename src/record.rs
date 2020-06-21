use std::{io::Write, mem::size_of};

/// Serialization interface for different ways to serialize `Record`.
pub trait RecordSerializer {
    /// Serialize the record and write it into the provided slice. The slice
    /// must have enough space to fit this recors.
    fn serialize(r: &Record, w: &mut [u8]);

    /// Try to deserialize a record. None is returned upon failure.
    fn deserialize<'a>(r: &'a [u8]) -> Option<Record<'a>>;

    /// The number of bytes this record will occupy on the drive.
    fn size(r: &Record) -> usize;
}

/// A database record.
pub struct Record<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

/// A record serialized in a form of:
///
/// * key length - 8 bytes
/// * value length - 8 bytes
/// * key bytes
/// * value bytes
///
/// Length values are recorded as little-endian. They are located next to each
/// other to make use of CPU caches.
pub struct BasicRecordSerializer;

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

impl RecordSerializer for BasicRecordSerializer {
    fn serialize(r: &Record, mut w: &mut [u8]) {
        w.write_all(&(r.key().len() as u64).to_le_bytes()[..])
            .unwrap();
        w.write_all(&(r.value().len() as u64).to_le_bytes()[..])
            .unwrap();
        w.write_all(&r.key()).unwrap();
        w.write_all(&r.value()).unwrap();
    }

    fn deserialize<'a>(mut r: &'a [u8]) -> Option<Record<'a>> {
        if r.len() < size_of::<u64>() * 2 {
            return None;
        }

        let mut key_length_bytes = [0u8; size_of::<u64>()];
        key_length_bytes.copy_from_slice(&r[..size_of::<u64>()]);
        let key_length = u64::from_le_bytes(key_length_bytes) as usize;
        r = &r[size_of::<u64>()..];

        let mut value_length_bytes = [0u8; size_of::<u64>()];
        value_length_bytes.copy_from_slice(&r[..size_of::<u64>()]);
        let value_length = u64::from_le_bytes(value_length_bytes) as usize;
        r = &r[size_of::<u64>()..];

        if r.len() < key_length + value_length {
            return None;
        }

        let key = &r[..key_length];
        r = &r[key_length..];

        let value = &r[..value_length];

        Some(Record { key, value })
    }

    fn size(r: &Record) -> usize {
        r.key.len() + r.value.len() + size_of::<u64>() * 2
    }
}

#[cfg(test)]
mod tests {
    use super::{BasicRecordSerializer, Record, RecordSerializer};
    use crate::testutils::TestData;

    #[quickcheck]
    fn serialization_sanity(data: TestData) -> bool {
        let record = Record::new(&data.key, &data.value);
        let mut slice = vec![0u8; BasicRecordSerializer::size(&record)];
        BasicRecordSerializer::serialize(&record, &mut slice);
        let deser_output = BasicRecordSerializer::deserialize(&slice).unwrap();
        data.key.as_slice() == deser_output.key() && data.value.as_slice() == deser_output.value()
    }
}

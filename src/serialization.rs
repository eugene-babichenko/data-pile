use crate::Record;
use std::{io::Write, mem::size_of};

/// Serialization interface for different ways to serialize `Record`.
pub trait RecordSerializer {
    /// Serialize the record and write it into the provided slice. The slice
    /// must have enough space to fit this recors.
    fn serialize(&self, r: &Record, w: &mut [u8]);

    /// Try to deserialize a record. None is returned upon failure.
    fn deserialize<'a>(&self, r: &'a [u8]) -> Option<Record<'a>>;

    /// The number of bytes this record will occupy on the drive.
    fn size(&self, r: &Record) -> usize;
}

impl<T: RecordSerializer> RecordSerializer for &T {
    fn serialize(&self, r: &Record, w: &mut [u8]) {
        (*self).serialize(r, w)
    }

    fn deserialize<'a>(&self, r: &'a [u8]) -> Option<Record<'a>> {
        (*self).deserialize(r)
    }

    fn size(&self, r: &Record) -> usize {
        (*self).size(r)
    }
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
#[derive(Clone)]
pub struct BasicRecordSerializer;

/// A record serialized in a form of:
///
/// * value length - 8 bytes
/// * key bytes (specified in advance)
/// * value bytes
#[derive(Clone)]
pub struct ConstKeyLenRecordSerializer {
    key_length: usize,
}

impl RecordSerializer for BasicRecordSerializer {
    fn serialize(&self, r: &Record, mut w: &mut [u8]) {
        w.write_all(&(r.key().len() as u64).to_le_bytes()[..])
            .unwrap();
        w.write_all(&(r.value().len() as u64).to_le_bytes()[..])
            .unwrap();
        w.write_all(&r.key()).unwrap();
        w.write_all(&r.value()).unwrap();
    }

    fn deserialize<'a>(&self, mut r: &'a [u8]) -> Option<Record<'a>> {
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

        Some(Record::new(key, value))
    }

    fn size(&self, r: &Record) -> usize {
        r.key().len() + r.value().len() + size_of::<u64>() * 2
    }
}

impl ConstKeyLenRecordSerializer {
    pub fn new(key_length: usize) -> Self {
        Self { key_length }
    }
}

impl RecordSerializer for ConstKeyLenRecordSerializer {
    fn serialize(&self, r: &Record, mut w: &mut [u8]) {
        assert!(self.key_length == r.key().len());
        w.write_all(&(r.value().len() as u64).to_le_bytes()[..])
            .unwrap();
        w.write_all(&r.key()).unwrap();
        w.write_all(&r.value()).unwrap();
    }

    fn deserialize<'a>(&self, mut r: &'a [u8]) -> Option<Record<'a>> {
        if r.len() < self.key_length + size_of::<u64>() {
            return None;
        }

        let mut value_length_bytes = [0u8; size_of::<u64>()];
        value_length_bytes.copy_from_slice(&r[..size_of::<u64>()]);
        let value_length = u64::from_le_bytes(value_length_bytes) as usize;
        r = &r[size_of::<u64>()..];

        let key = &r[..self.key_length];
        r = &r[self.key_length..];

        if r.len() < value_length {
            return None;
        }

        let value = &r[..value_length];

        Some(Record::new(key, value))
    }

    fn size(&self, r: &Record) -> usize {
        self.key_length + r.value().len() + size_of::<u64>()
    }
}

#[cfg(test)]
mod tests {
    use super::{BasicRecordSerializer, ConstKeyLenRecordSerializer, RecordSerializer};
    use crate::{
        record::Record,
        testutils::{FixLenTestData, TestData},
    };

    #[quickcheck]
    fn serialization_sanity_basic(data: TestData) -> bool {
        let record = Record::new(&data.key, &data.value);
        let mut slice = vec![0u8; BasicRecordSerializer.size(&record)];
        BasicRecordSerializer.serialize(&record, &mut slice);
        let deser_output = BasicRecordSerializer.deserialize(&slice).unwrap();
        data.key.as_slice() == deser_output.key() && data.value.as_slice() == deser_output.value()
    }

    #[quickcheck]
    fn serialization_sanity_const_key_len(data: FixLenTestData) -> bool {
        let serializer = ConstKeyLenRecordSerializer::new(32);
        let record = Record::new(&data.key, &data.value);
        let mut slice = vec![0u8; serializer.size(&record)];
        serializer.serialize(&record, &mut slice);
        let deser_output = serializer.deserialize(&slice).unwrap();
        &data.key == deser_output.key() && data.value.as_slice() == deser_output.value()
    }
}

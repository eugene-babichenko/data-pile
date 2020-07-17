use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use data_pile::{serialization::BasicRecordSerializer, Database, Error, Record};
use rand::{rngs::OsRng, RngCore};

fn put_get(c: &mut Criterion) {
    const BATCH_SIZE: usize = 2048;
    const MAX_KEY_LEN: u64 = 512;
    const MAX_VALUE_LEN: u64 = 4096;

    let tmp = tempfile::tempdir().unwrap();
    let db = Database::new(tmp.path(), BasicRecordSerializer).unwrap();

    let mut rng = OsRng;

    let mut keys = Vec::new();

    c.bench_function(&format!("put {} records per iteration", BATCH_SIZE), |b| {
        b.iter_batched(
            || {
                let mut data: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(BATCH_SIZE);

                for _i in 0..BATCH_SIZE {
                    let key_len = (rng.next_u64() % MAX_KEY_LEN) as usize;
                    let value_len = (rng.next_u64() % MAX_VALUE_LEN) as usize;

                    let mut key = vec![0u8; key_len];
                    let mut value = vec![0u8; value_len];

                    rng.fill_bytes(&mut key);
                    rng.fill_bytes(&mut value);

                    data.push((key, value));
                }

                data
            },
            |data| {
                let records: Vec<_> = data
                    .iter()
                    .map(|data| Record::new(&data.0, &data.0))
                    .collect();
                match db.append(&records) {
                    Ok(_) => {
                        let keys_new: Vec<_> = data.into_iter().map(|data| data.0).collect();
                        keys.push(keys_new);
                    }
                    Err(Error::RecordExists(_)) => {}
                    Err(e) => panic!(e),
                }
            },
            BatchSize::PerIteration,
        );
    });

    let keys: Vec<_> = keys.into_iter().flatten().collect();

    c.bench_function("read random records", |b| {
        b.iter(|| {
            let i = (rng.next_u64() as usize) % keys.len();
            let _record = db.get(&keys[i]).unwrap();
        });
    });

    let mut iter = db.iter_from_seqno(0).unwrap();

    c.bench_function("read consecutive records", |b| {
        b.iter(|| {
            let _maybe_record = iter.next();
        });
    });
}

criterion_group!(benches, put_get);
criterion_main!(benches);

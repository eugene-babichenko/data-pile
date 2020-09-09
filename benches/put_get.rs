use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use data_pile::Database;
use rand::{rngs::OsRng, RngCore};

fn put_get(c: &mut Criterion) {
    const BATCH_SIZE: usize = 2048;
    const MAX_VALUE_LEN: u64 = 4096;

    let tmp = tempfile::tempdir().unwrap();
    let db = Database::file(tmp.path()).unwrap();

    let mut rng = OsRng;

    let mut number = 0;

    c.bench_function(&format!("put {} records per iteration", BATCH_SIZE), |b| {
        b.iter_batched(
            || {
                let mut data: Vec<Vec<u8>> = Vec::with_capacity(BATCH_SIZE);

                for _i in 0..BATCH_SIZE {
                    let value_len = (rng.next_u64() % MAX_VALUE_LEN) as usize;
                    let mut value = vec![0u8; value_len];
                    rng.fill_bytes(&mut value);

                    data.push(value);
                }

                number += BATCH_SIZE;

                data
            },
            |data| {
                let records: Vec<_> = data.iter().map(|data| data.as_ref()).collect();
                db.append(&records).unwrap();
            },
            BatchSize::PerIteration,
        );
    });

    c.bench_function("read random records", |b| {
        b.iter(|| {
            let i = (rng.next_u64() as usize) % number;
            let _record = db.get_by_seqno(i).unwrap();
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

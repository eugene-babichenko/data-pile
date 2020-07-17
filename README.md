# `pile` - a simple and fast append-only data store

## Design goals

* Efficient adding of bug chunks of data.
* A user should be able to copy the storage data (for example, over the network)
  while still being able to use the database for both reads and writes.
* The storage should have a minimal dependency footprint.

## Usage guide

### Example

```rust
use data_pile::{Database, BasicRecordSerializer};

let db = Database::new(BasicRecordSerializer, "./pile");

let key = b"qwerty";
let value = b"some data";

let record = Record::new(&key, &value);

db.put(record).unwrap();
```

### Transferring the data

- Get the raw data by using `Database::snapshot()`.
- Copy it somewhere.
- Create the root direcotry of the data store, create a file named `data` in it
  and add all snapshot data into it.
- Just start using the database: it will verify correctness and rebuild all
  indexes.

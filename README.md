# `pile` - a simple and fast append-only data store

[![Crates.io](https://img.shields.io/crates/v/data-pile)](https://crates.io/crates/data-pile)
[![codecov](https://codecov.io/gh/eugene-babichenko/data-pile/branch/master/graph/badge.svg)](https://codecov.io/gh/eugene-babichenko/data-pile)

## Design goals

* Efficient adding of big chunks of data.
* A user should be able to copy the storage data (for example, over the network)
  while still being able to use the database for both reads and writes.
* The storage should have a minimal dependency footprint.

## Usage guide

### Example

```rust
use data_pile::Database;
let db = Database::file("./pile").unwrap();
let value = b"some data";
db.put(&value).unwrap();
```

### Notes

Values are accessible only by their sequential numbers. You will need an
external index if you want any other kind of keys.

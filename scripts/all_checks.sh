#! /bin/sh

set -ex

cargo fmt --all -- --check
cargo clippy
cargo bench --features with-bench --no-run
cargo test

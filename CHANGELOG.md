# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2020-08-19
### Changed
- Changed the internal data format to remove lengths from the flatfile.
- Zero-length entries are not legal now and will cause panics.
### Removed
- Snapshot capability due to breaking changes in the data format. Now users
  should just copy the whole directory.

## [0.3.1] - 2020-08-11
### Added
- Implement `Debug` for `SharedMmap`.

## [0.3.0] - 2020-08-11
### Changed
- Public methods now use `SharedMmap` instead of `&[u8]`.
- `SeqNoIter` now also uses `SharedMmap` which allows it to use the `Iterator`
  trait.

## [0.2.0] - 2020-08-04
### Removed
- Indexing by key - now records can only be indexed by their sequential number.
  This also allows to remove serializers and `Record` type.

## [0.1.1] - 2020-07-22
### Fixed
- Non-existent database location is actually created

## [0.1.0] - 2020-07-22
### Added
- Basic cross-platform flat storage.
- Persistent indexing by record number.
- In-memory B-tree for indexing by keys.
- Possibility to have different record serialization approaches.

[Unreleased]: https://github.com/eugene-babichenko/data-pile/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/eugene-babichenko/data-pile/releases/tag/v0.4.0
[0.3.1]: https://github.com/eugene-babichenko/data-pile/releases/tag/v0.3.1
[0.3.0]: https://github.com/eugene-babichenko/data-pile/releases/tag/v0.3.0
[0.2.0]: https://github.com/eugene-babichenko/data-pile/releases/tag/v0.2.0
[0.1.1]: https://github.com/eugene-babichenko/data-pile/releases/tag/v0.1.1
[0.1.0]: https://github.com/eugene-babichenko/data-pile/releases/tag/v0.1.0

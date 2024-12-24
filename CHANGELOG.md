# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] - 2021-12-24

### Fixed

- Primary key for `metadata` table is now a composite key of `mmsi` and `timestamp`


## [0.1.0] - 2021-12-23

### Added

- Initial version of the project
- Basic functionality for recording AIS messages
- Writing messages to a database
- Daily export to Parquet files
- Basic logging functionality
- Basic configuration handling
- Basic error handling
- Basic unit tests

[Unreleased]: https://github.com/mkouhia/ais-recorder/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/mkouhia/ais-recorder/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/mkouhia/ais-recorder/releases/tag/v0.1.0

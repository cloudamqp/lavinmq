# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.8.2] - 2019-07-21

### Changed
- Faster queue matching on publishing by reusing a Set
- Faster GC of unused message store segments
- Do not enable TCP_NODELAY
- Faster avalanchemqproxy

## [0.8.1] - 2019-07-19

### Changed
- Restored fairness between publishers
- Use a SparseArray as channel store, for faster access times

## [0.8.0] - 2019-07-16

### Added
- Fsyncing message store and queue index if publish confirm, when idle or every 200ms

### Changed
- AMQ::Protocol::Table is now backed by an IO::Memory and only parsed on-demand

## [0.7.13] - 2019-07-07

### Changed
- Separate buffer_size configuration for files and sockets
- Fsync when writing defintions, vhosts and users

## [0.7.12] - 2019-06-28

### Added
- Make buffer_size configurable via config and increase default from 8KB to 128KB
- Pre-allocate the max size for new segments, might improve write performance (on Linux only)

## [0.7.11] - 2019-06-20

### Changed
- Revert to write lock behavior, but yield only when queue length > 10000

## [0.7.10] - 2019-06-19

### Added
ShortStrings are now added to StringPool for reduced GC pressure, the pool size is printed on HUP

### Changed
- Make setting the timestamp property optional through a config setting (set_timestamp), default to false
- Optimized topic exchange and fanout routing, giving ~5% and ~30% throughput boost respectively
- Using a Channel to communicate between Client and Vhost when publishing, removed the need for a lock

### Fixed
- AMQPLAIN support in shovels is implemented correctly

## [0.7.9] - 2019-06-10

### Fixed
- Restore ability to login with AMQPLAIN mechanism
- Making a binding via the HTTP API without a routing key threw an exception

## [0.7.8] - 2019-06-08

### Fixed
- Fix account of position in delivery loop so that we don't have to seek on disk

### Changed
- Optimize disconnection of consumers with many unacked messages

## [0.7.7] - 2019-06-07

### Fixed
- Always copy msg body to memory before writing to disk to avoid corruption if client disconnects mid-stream
- Reset position of segment when an exception is caught in delivery loop

### Changed
- chmod the unix domain socket to 777, authentication is done on the protocol level anyway

### Added
- CHANGELOG file

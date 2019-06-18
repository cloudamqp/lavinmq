# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
ShortStrings are now added to StringPool for reduced GC pressure, the pool size is printed on HUP

### Changed
- Make setting the timestamp property optional through a config setting (set_timestamp), default to false

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

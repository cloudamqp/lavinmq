# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.8] - 2019-06-08
- Optimize disconnection of consumers with many unacked messages
- Fix account of position in delivery loop so that we don't have to seek on disk

## [0.7.7] - 2019-06-07
- Always copy msg body to memory before writing to disk to avoid corruption if client disconnects mid-stream
- Reset position of segment when an exception is caught in delivery loop
- chmod the unix domain socket to 777, authentication is done on the protocol level anyway
- CHANGELOG file

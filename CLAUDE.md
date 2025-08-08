# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

LavinMQ is a high-performance message queue server implementing AMQP 0-9-1 and MQTT 3.1.1 protocols, built with Crystal. It features clustering, stream queues, federation, and extensive AMQP functionality.

## Development Commands

### Building
- `make` - Build all binaries (lavinmq, lavinmqctl, lavinmqperf) in release mode
- `make bin/lavinmq CRYSTAL_FLAGS=` - Build debug version of the main server binary
- `make bin/lavinmq-debug` - Build with debug symbols
- `make deps` - Install dependencies (shards and JavaScript assets)

### Testing
- `make test` - Run all specs (use instead of "crystal spec")
- `make test SPEC=spec/specific_test.cr` - Run specific test file
- `make format` - Check Crystal code formatting
- `make lint` - Run Ameba linter on src/ and spec/

### Development Tools
- `make watch` - Watch for file changes and rebuild automatically
- `make dev-ui` - Start development UI with live reload for web assets

## Architecture

### Core Components
- **Server (`src/lavinmq/server.cr`)** - Main server implementation handling AMQP and HTTP
- **VHost (`src/lavinmq/vhost.cr`)** - Virtual host management with isolated exchanges and queues
- **Exchange (`src/lavinmq/exchange.cr`)** - Message routing with multiple exchange types (direct, topic, fanout, headers, consistent-hash)
- **Queue (`src/lavinmq/queue.cr`)** - Message storage with support for durable, priority, and stream queues
- **Message Store (`src/lavinmq/message_store.cr`)** - Disk-based message persistence using segment files

### Protocol Support
- **AMQP (`src/lavinmq/amqp/`)** - Full AMQP 0-9-1 implementation with channels, consumers, and transactions
- **MQTT (`src/lavinmq/mqtt/`)** - MQTT 3.1.1 broker with topic routing and retained messages
- **HTTP API (`src/lavinmq/http/`)** - Management API and web UI

### Clustering
- **Clustering (`src/lavinmq/clustering/`)** - Multi-node replication with etcd-based leader election
- **Replicator** - Streams changes between leader and followers in real-time

### Key Features
- **Stream Queues** - Append-only logs for event sourcing with filtering support
- **Federation** - Cross-cluster message routing via upstream links
- **Shovels** - Message forwarding between queues and exchanges
- **Authentication** - Pluggable auth system with password-based authentication

## File Organization

- `src/lavinmq.cr` - Main entry point, loads config and starts launcher
- `src/lavinmq/launcher.cr` - Initializes and coordinates all server components
- `src/lavinmq/config.cr` - Configuration parsing from files and command line
- `src/stdlib/` - Crystal standard library extensions and optimizations
- `spec/` - Test suite with extensive AMQP, MQTT, and clustering tests
- `static/` - Web UI assets (HTML, CSS, JavaScript)
- `views/` - ECR templates for web interface

## Development Workflow

1. Make changes to Crystal source files
2. Run `make test` to verify functionality
3. Use `make lint` and `make format` to ensure code quality
4. Build with `make` for release or `make bin/lavinmq CRYSTAL_FLAGS=` for debugging
5. Test with `./bin/lavinmq -D /tmp/data` for local development

## Performance Considerations

LavinMQ uses a disk-first approach where the OS handles caching. Message segments are memory-mapped files, and the server is designed for high throughput with minimal memory usage.

## Code Review Guidelines

When conducting code reviews, Claude should:

### Review Style
- Provide concise, actionable feedback
- Focus on critical issues: bugs, security vulnerabilities, performance problems
- Highlight Crystal-specific best practices and idioms
- Check for proper error handling and resource cleanup
- Verify thread safety in concurrent code

### Key Areas to Review
- **Memory Safety** - Check for potential memory leaks, especially with C bindings
- **AMQP Compliance** - Ensure protocol implementations follow AMQP 0-9-1 specification
- **Performance** - Look for inefficient algorithms, unnecessary allocations, blocking operations
- **Testing** - Verify adequate test coverage for new functionality

### Review Format
- Use bullet points for multiple issues
- Reference specific line numbers when possible
- Suggest concrete improvements rather than just identifying problems
- Keep feedback under 200 words per file unless critical issues require detailed explanation
- always mention which file if referencing lines

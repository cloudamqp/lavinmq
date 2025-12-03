# CLAUDE.md

Message queue server implementing the AMQP 0-9-1 and MQTT 3.1.1 protocols, built with Crystal.

## Development Workflow
1. Make changes to Crystal source files
2. Run individual spec with `make test SPEC=spec/new_spec.cr`
3. Run `make test` to run all specs to ensure no regressions
4. Run linter with `make lint`
5. Checkout a new branch for your changes
6. Commit changes with short message
7. Open a draft pull request at GitHub for review

## Running locally
- `make bin/lavinmq CRYSTAL_FLAGS=` - Build debug version of the main server binary
- `bin/lavinmq --data-dir ./tmp/data --debug` - Run server with specified data directory in debug logging mode

## File Organization
- `src/lavinmq.cr` - Entry point
- `src/lavinmq/launcher.cr` - Coordinates all server components
- `src/stdlib/` - Crystal stdlib extensions

Standard library sources: `crystal env CRYSTAL_PATH` shows compiler search paths.

## Key Patterns
- Avoid allocations in hot paths (message publishing and delivery)
- SEGFAULTs due to use of MFiles after close is a common source of bugs

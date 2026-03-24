# CLAUDE.md

## Commands
- `make test SPEC=spec/foo_spec.cr` — run a single spec
- `make test` — run all specs
- `make lint` — run linter

## Key Patterns
- Avoid allocations in hot paths (message publishing and delivery)
- SEGFAULTs due to use of MFiles after close is a common source of bugs
- All code changes must have corresponding specs. New features need specs, bug fixes need regression specs.

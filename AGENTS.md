# CLAUDE.md

LavinMQ: message queue and streaming server implementing AMQP 0-9-1 and MQTT 3.1.1, written in Crystal.

## Commands
- `make test SPEC=spec/server_spec.cr` — run one spec file (prefer this while iterating)
- `make test` — full suite (slow; run once at the end)
- `make lint` — linter; `crystal tool format` — formatter
- `make bin/lavinmq CRYSTAL_FLAGS=` — debug build
- `bin/lavinmq --data-dir ./tmp/data --debug` — run locally
- `crystal env CRYSTAL_PATH` — where stdlib sources live; read them instead of guessing APIs

## Architecture
- Server owns listeners and vhosts; each VHost isolates exchanges, queues, bindings, permissions, and policies.
- Exchanges route through bindings to queues, including exchange-to-exchange bindings. Queues manage delivery and acknowledgements; MessageStore handles disk-backed message storage.
- Publish: Client → Channel → VHost → Exchange → Queue → MessageStore.
- Delivery: Consumer pulls from Queue/MessageStore → Channel → Client; respects prefetch and flow control.
- Clustering: One leader, multiple followers; leader replicates state to followers. Leader election uses Raft; followers can be promoted to leader if they are in the ISR (in-sync replica) set.
- Stream queues retain messages and give consumers independent offsets.
- See CONTRIBUTING.md for storage layout and implementation details.

## Repository map
- src/lavinmq/ — broker implementation
- src/lavinmq/clustering/ — replication and cluster coordination
- src/lavinmqctl/ — administration CLI
- src/lavinmqperf/ — benchmark CLI
- spec/ — Crystal specs
- spec/frontend/ — Playwright tests
- views/ — templates used to generate static HTML, generate with `make views`
- static/js/ — management UI JavaScript
- docs/ — feature documentation
- openapi/ and static/docs/ — HTTP API documentation

## When reviewing a PR
Review the diff and relevant callers, implementations, and specs; don't
build or run specs unless requested. Focus on logical errors, hot-path
allocations, memory safety, concurrency/fiber safety, and missing or weak
specs. Skip style issues the linter catches. Don't use emojis.

## Docs
- Update CHANGELOG.md with a short summary of the change, and link to the PR.
- Update docs/ with a longer description of the change, if it's a user facing feature.

## Specs
- Add specs for behavior that could plausibly break: protocol semantics, persistence and
  recovery, concurrency, edge cases, and bug fixes (a regression spec that fails before the fix).
- Don't add specs for trivial code: simple getters, pure refactors with existing coverage,
  config plumbing, log messages, or anything the type system already guarantees.
- Prefer extending an existing spec over writing a new one. Only create a new spec file
  for a new component.
- One focused spec that exercises real behavior beats several that each check one line.
- If existing specs already cover the change, say so instead of adding one.

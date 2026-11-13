LavinMQ: message queue and streaming server (AMQP 0-9-1, MQTT 3.1.1) in Crystal.

## Commands
- `make test SPEC=spec/foo_spec.cr` — one spec (iterate with this); `make test` — full suite in parallel processes, once at the end (`SPEC_JOBS=1 SPEC_SEED=<seed>` with the seed it prints runs the same order in one process)
- `make lint`; `crystal tool format`
- `make bin/lavinmq CRYSTAL_FLAGS=` — debug build; `bin/lavinmq --data-dir ./tmp/data --debug` — run
- Frontend (views/, static/js/): `make views`, then `make test-frontend [SPEC=spec/frontend/foo.spec.js]` against a running server
- `crystal env CRYSTAL_PATH` — stdlib sources; read them instead of guessing APIs

## Architecture
- Clustering: one leader replicates to followers; Raft election; only ISR members can be promoted.
- Stream queues retain messages with per-consumer offsets.
- views/ generates static/*.html; HTTP API docs in openapi/ and static/docs/.
- Storage layout and internals: CONTRIBUTING.md.

## Rules
- No allocations, per-message fibers, or polling in hot paths (publish, delivery, MessageStore).
- Message data is mmap'd segments exposed as raw Pointers; reading after the mmap closes segfaults rather than raising.
- New code and refactors must be thread safe: avoid unprotected shared mutable state. Do not copy the single-thread assumptions that some existing code still has.

## Specs
- Add specs for behavior that could plausibly break (protocol semantics, persistence/recovery, concurrency, edge cases); bug fixes get a regression spec that fails before the fix.
- Skip trivial code (getters, covered refactors, config plumbing); extend existing spec files rather than adding new ones.

## Docs
- Add a short CHANGELOG.md entry linking the PR; describe user-facing features in docs/.
- Keep each CHANGELOG.md section (Added, Changed, Fixed) ordered by user impact, most impactful first: insert a new entry where it ranks, not at the end. New features, behaviour changes users must act on, and fixes for data loss, crashes or a stopped broker go before performance tweaks, metrics, management UI polish and build or packaging changes.

## Git
- Branch names describe the change in kebab-case, e.g. `fix-queue-ttl-overflow`; rename a generated branch name with `git branch -m` before the first push.
- Write commits, PR descriptions and comments as the person you work for: never change git `user.name`/`user.email` or pass `--author`, and add no AI attribution (Co-Authored-By trailers for the agent, "Generated with" footers).

# Docs Check Criteria

## Your job

Decide whether this PR's code changes require accompanying updates to `docs/`. Emit a single JSON verdict to `/tmp/docs-verdict.json`. Do not post any comments. Do not push code changes.

## Steps

1. Run `gh pr diff $PR_NUMBER` to read the changes.
2. Identify any user-facing surfaces affected by the diff (see "What counts as user-facing" below).
3. If no user-facing changes are detected, write a `pass` verdict and stop.
4. If user-facing changes are detected, inspect the relevant `docs/` files (see "Doc surface map") and any docs touched in this PR's diff.
5. Decide between `pass` (docs updated and sufficient), `neutral` (docs touched but incomplete), or `fail` (docs not touched at all).
6. Write the verdict to `/tmp/docs-verdict.json` with the exact shape shown under "Output".

You have a maximum of 20 turns. Finish well within that limit. Only read code or docs that you actually need to make the judgment.

## What counts as user-facing

A change is user-facing if it affects any of:

- Configuration values (env vars, config file keys, defaults, valid value ranges).
- CLI commands or flags for `lavinmqctl`, `lavinmqperf`, or the `lavinmq` server itself.
- HTTP API endpoints, request parameters, response shapes, or status codes.
- AMQP 0-9-1 or MQTT 3.1.1 protocol behavior visible to clients (frame handling, error responses, supported features).
- Observable runtime behavior users may rely on: log fields, log levels for known events, metric names, error messages surfaced to users, on-disk file or directory layout in the data directory.
- New user-discoverable features (delayed queues, deduplication, federation, shovels, etc.).
- Default value changes for any of the above.

## What does NOT count as user-facing

- Internal refactors with no observable behavior change.
- Performance optimizations that don't change semantics or surfaces.
- Test-only changes (anything under `spec/`).
- Comment or typo fixes in code.
- Build, CI, or tooling changes.
- Changes to private or internal classes not exposed through any user-facing surface above.

## Doc surface map

When deciding sufficiency, check the file in `docs/` that matches the changed surface:

- `docs/configuration.md` for config values and defaults.
- `docs/lavinmqctl.md` for `lavinmqctl` commands and flags.
- `docs/lavinmqperf.md` for `lavinmqperf` flags and behavior.
- `docs/monitoring.md` for metrics, log fields, and observability surfaces.
- `docs/amqp.md` for AMQP protocol behavior.
- `docs/mqtt.md` for MQTT protocol behavior.
- For any other surface, glob `docs/*.md` and pick the file whose title matches the changed component. If no matching doc file exists for a new user-facing feature, that counts as missing documentation.

## Verdict rules

- `pass`: no user-facing changes detected, OR user-facing changes exist and are documented sufficiently in this PR.
- `neutral`: user-facing changes detected, the PR touches at least one file under `docs/`, but the documentation updates are incomplete (for example: new flag added but its description is missing; default value changed but old value still shown; new feature added but only mentioned in a single line without explaining usage).
- `fail`: user-facing changes detected and no file under `docs/` was touched in this PR.

## Output

Write exactly this file:

Path: `/tmp/docs-verdict.json`
Content:

```json
{"verdict": "pass" | "neutral" | "fail", "reason": "<one sentence explaining the decision, max 200 characters>"}
```

The `reason` text appears verbatim in the PR check summary. Be specific: name the file, feature, or flag that triggered the verdict.

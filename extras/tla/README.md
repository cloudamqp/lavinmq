# TLA+ specs for clustering

`MessagesOnRaft.tla` specifies the proposed replication for LavinMQ, where the broker's data goes
through one Raft log instead of a file stream next to an ISR kept by Raft. It's model checked
before the implementation is written, to find design mistakes while they're cheap to fix, and
later the implementation is checked against it.

## Running

```sh
make tla                 # the quick models, as CI runs them
make tla TIER=full       # also the larger ones, which take longer
extras/tla/check.sh full MC_durable   # one model
```

`check.sh` downloads TLC (`tla2tools.jar`, a pinned release checked against its SHA-256) into
`extras/tla/.cache/` and needs Java 11 or later. `TLA_WORKERS` sets TLC's worker threads (all
cores by default), `TLA_SIM_SECONDS` how long simulation models run (120 s).

The TLA+ Toolbox or the VS Code extension can open the specs too: check `MC.tla` with one of the
`MC_*.cfg` files.

## What's modelled

A log entry is an operation on the broker's files: a publish (append to a segment), a consumer's
ack (append to the ack file), a segment delete, or a new leader's no-op. Each server has its log
as appended (in memory and the page cache) and how much of it is fsynced.

- Append-only operations take effect when appended and are undone when truncated. In the
  implementation the files are truncated back to the position recorded in the entry.
- A delete can't be undone, so it's carried out only once the entry is committed and fsynced
  locally.
- **Confirms** wait for the commit index: entries of the leader's term fsynced on a majority. The
  leader that took the publish sends the confirm.
- **Deliveries** wait for `DeliveryGate`:
  - `durable`: the commit index;
  - `replicated`: a majority holds the entry, fsynced or not;
  - `none`: no wait, as the `raft-election` branch delivers today.
- **Failures**:
  - replication steps are skipped (lost messages);
  - `Restart`: the process restarts and loses its volatile state, the page cache survives;
  - `PowerLoss`: the machine loses power and loses what it hadn't fsynced.
- **`AbstainAfterPowerLoss`**: after a power loss a server doesn't vote or campaign until it has
  caught up with a leader's whole log. It may have lost entries it had acknowledged, and only
  then does it hold them all again.
  - If no majority is left that may vote, servers give up abstaining (`GiveUpAbstaining`), because
    otherwise the cluster would never elect a leader again. The spec records that as
    `unsafeRecovery`.
  - The implementation tells a power loss from a process restart by the boot id.

### Abstractions

These keep the state space small enough to check, without hiding behaviour that matters for
safety:

- **An election is one step.** Each voter in the winning quorum grants its vote by Raft's rules: it
  hasn't voted in the term yet, the candidate's log is at least as up to date, and it isn't
  abstaining. A vote binds the voter for the whole term, so it makes no difference to safety when
  the votes are counted. Failed elections are left out; they only raise terms.
- **A replication round trip is one step**, at most one entry starting after any index of the
  leader's log. That covers whatever its next_index logic sends.
  - A delayed AppendEntries carries a prefix of the leader's log that the leader could also send
    now, or it's rejected for its term, the same as a lost one.
  - A delayed ack is the same as a later commit step, because the leader only raises match indexes.
  - A follower acks what it fsynced on the next round trip.
- **Not modelled:**
  - membership changes;
  - pre-vote, check-quorum and leadership transfer, which only refuse votes or start elections
    earlier;
  - log compaction and snapshots;
  - liveness.

## Invariants

| Invariant | Meaning |
|---|---|
| `ElectionSafety` | At most one leader per term |
| `LeaderCompleteness` | Every leader of a later term has every committed entry |
| `ConfirmedNotLost` | Every later leader has a confirmed message, and has it on disk unless it was acked |
| `NoPhantomDelivery` | Every later leader has a delivered message, unless a majority lost power and gave up abstaining |
| `NoPrematureDelete` | A file is only deleted for a committed delete |

## Models

Each `MC_*.cfg` states in its header what TLC must find (`Expect`), how it searches (`Mode`:
exhaustive `bfs`, or `simulate` for random behaviours), and whether CI runs it (`Tier`).

The `MC_bug_*` models re-introduce a known mistake each, and TLC must find a counterexample. That
shows the invariants actually catch the problems they're there for.

| Model | Tier | Expect | Result |
|---|---|---|---|
| `MC_durable` | full | pass | 58.6M distinct states, depth 37, about 80 min on 4 cores |
| `MC_replicated_abstain` | full | pass | not completed: no violation in 128M distinct states (depth 22) in 2 h, with 44M still queued. Tracking which follower holds what in memory makes the state space far larger than `MC_durable`'s |
| `MC_durable_small` | quick | pass | 1.4M distinct states, about 2 min |
| `MC_replicated_abstain_small` | quick | pass | 1.8M distinct states, about 2 min |
| `MC_durable_sim`, `MC_replicated_abstain_sim` | quick | pass | random behaviours for 2 min each, up to 4 terms and 2 crashes |
| `MC_bug_no_gate` | quick | `NoPhantomDelivery` | found in seconds |
| `MC_bug_replicated_no_abstain` | quick | `NoPhantomDelivery` | found in seconds |
| `MC_bug_stop_abstain_on_contact` | quick | `NoPhantomDelivery` | found in seconds |
| `MC_bug_ack_before_fsync` | quick | `LeaderCompleteness` | found in seconds |
| `MC_bug_uncapped_durable_ack` | quick | `LeaderCompleteness` | found in seconds |
| `MC_bug_eager_delete` | quick | `NoPrematureDelete` | found in seconds |
| `MC_bug_no_term_check` | quick | `LeaderCompleteness` | found in seconds (needs 4 terms) |

All models use 3 servers, 1 message and 1 segment file. The full models allow 3 terms, 4 log
entries and 1 crash. The small ones allow 2 terms, the replicated one also only 3 entries.

## What it found

- **Delivering on majority replication alone loses delivered messages.** In
  `MC_bug_replicated_no_abstain`:
  1. A follower acks a message it hasn't fsynced, and the leader delivers it.
  2. The follower loses power and loses the message.
  3. The follower votes for a candidate that lacks the message.

  That takes one power loss, not a majority losing power at once.
- **Abstaining until the server has heard from a leader isn't enough**
  (`MC_bug_stop_abstain_on_contact`): a heartbeat that doesn't carry the lost entries would end
  the abstention. The server has to abstain until it has caught up with the leader's whole log.
- **What remains with replicated delivery:** if a majority loses power together, it either stays
  without a leader or gives up abstaining, and then delivered messages can be lost.
  `MC_replicated_abstain` checks that nothing else can lose them. Delivering on the durable commit
  index avoids this case, at the cost of an fsync in the delivery latency.
- **A follower's durable ack must be capped at what it knows matches the leader's log**
  (`MC_bug_uncapped_durable_ack`). Otherwise fsynced entries left over from an earlier term are
  counted for the leader's entries at the same index.

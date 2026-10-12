--------------------------- MODULE MessagesOnRaft ---------------------------
(***************************************************************************)
(* Messages on Raft: the broker's data (message segments, ack files, file  *)
(* deletes) replicated through one Raft log, as proposed for LavinMQ's     *)
(* clustering.                                                             *)
(*                                                                         *)
(* A log entry is an operation on the broker's files. Message bytes live   *)
(* in the segment files and the log only describes them, but here a file's *)
(* content is simply what the log says was appended to it: append-only     *)
(* operations take effect when an entry is appended and are undone when it *)
(* is truncated (files are truncated back to the recorded position).       *)
(* Destructive operations (deleting a file) can't be undone, so they are   *)
(* carried out once the entry is committed and fsynced locally. That rule, *)
(* when publishes are confirmed and when messages may be delivered are     *)
(* what this spec checks.                                                  *)
(*                                                                         *)
(* Two indexes on the leader:                                              *)
(*  - commitIndex: entries of its term fsynced on a majority. Confirms     *)
(*    and destructive operations wait for it.                              *)
(*  - ReplIndex: entries of its term held by a majority, fsynced or not.   *)
(*    Deliveries may wait only for that, see DeliveryGate.                 *)
(*                                                                         *)
(* Failures: replication steps are skipped (lost messages), servers        *)
(* restart (losing their volatile state) and lose power (also losing what  *)
(* they hadn't fsynced). The term is fsynced before anything that depends  *)
(* on it is sent, as Raft::Node does, so it's never lost.                  *)
(*                                                                         *)
(* To keep the state space small enough to check, an election and a       *)
(* replication round trip are each one step; README.md explains why that   *)
(* hides no behaviour that matters for safety. Not modelled: membership    *)
(* changes, pre-vote, check-quorum and leadership transfer (they only      *)
(* refuse votes or start elections earlier), log compaction and snapshots. *)
(*                                                                         *)
(* The Bug* constants re-introduce known mistakes; each must make TLC find *)
(* a counterexample, see the MC_bug_*.cfg models.                          *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Server, Msg, File,
    FileOf,                 \* the segment file each message is written to
    Nil,
    MaxTerm, MaxLogLen, MaxCrashes,
    \* What a delivery waits for: "durable" (commitIndex), "replicated"
    \* (ReplIndex) or "none" (delivered as soon as it's written, as the
    \* raft-election branch does)
    DeliveryGate,
    \* After losing power a server doesn't vote, nor campaign, until it has
    \* caught up with a leader's whole log: it may have lost entries it had
    \* acked, and only holds them all again then
    AbstainAfterPowerLoss,
    \* Servers stop abstaining as soon as they hear from a leader, caught up
    \* or not
    BugStopAbstainOnContact,
    \* Followers ack entries as durable before fsyncing them
    BugAckBeforeFsync,
    \* Followers ack their whole fsynced log as durable, not just the part
    \* verified to match the leader's
    BugUncappedDurableAck,
    \* Files are deleted as soon as the delete is appended, not committed
    BugEagerDelete,
    \* A leader commits entries of earlier terms by counting replicas
    \* (Raft paper, figure 8)
    BugNoTermCheck

ASSUME DeliveryGate \in {"durable", "replicated", "none"}
ASSUME FileOf \in [Msg -> File]

VARIABLES
    currentTerm,
    role,           \* "Follower" or "Leader"
    log,            \* the log as appended, in memory and the page cache
    durableLen,     \* how much of log is fsynced
    commitIndex,
    matchMem,       \* leader: what each follower holds, fsynced or not
    matchDur,       \* leader: what each follower has fsynced
    deleted,        \* files deleted from disk, which survives anything
    abstain,
    \* History, for the invariants only
    published,
    confirmed,          \* <<message, term of the leader that confirmed it>>
    delivered,          \* <<message, term of the leader that delivered it>>
    committed,          \* <<index, entry, term it was first committed in>>
    unsafeRecovery,     \* a majority lost power and gave up abstaining
    crashes

vars == <<currentTerm, role, log, durableLen, commitIndex, matchMem, matchDur,
          deleted, abstain, published, confirmed, delivered, committed,
          unsafeRecovery, crashes>>

-----------------------------------------------------------------------------
\* Helpers

Quorum == {Q \in SUBSET Server : Cardinality(Q) * 2 > Cardinality(Server)}
Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b
LastTerm(l) == IF Len(l) = 0 THEN 0 ELSE l[Len(l)].term

Op(type, arg) == [type |-> type, arg |-> arg]
NoopOp == Op("noop", Nil)
PubOp(m) == Op("pub", m)
AckOp(m) == Op("ack", m)
DelOp(f) == Op("del", f)

InLog(s, op) == \E i \in 1..Len(log[s]) : log[s][i].op = op
IndexOf(s, op) == CHOOSE i \in 1..Len(log[s]) : log[s][i].op = op

\* The highest index of the leader's term on a majority by `match`, counting
\* the leader with `own`, the length it has itself. 0 when there's none.
QuorumIndex(s, match, own) ==
    LET ok(i) == /\ BugNoTermCheck \/ log[s][i].term = currentTerm[s]
                 /\ {t \in Server \ {s} : match[t] >= i}
                        \cup (IF own >= i THEN {s} ELSE {}) \in Quorum
        good == {i \in 1..Len(log[s]) : ok(i)}
    IN IF good = {} THEN 0 ELSE CHOOSE i \in good : \A j \in good : j <= i

ReplIndex(s) == QuorumIndex(s, matchMem[s], Len(log[s]))

\* What a follower acks as durable: the fsynced part of what it now knows
\* matches the leader's log
DurableAck(durable, ver) ==
    IF BugAckBeforeFsync THEN ver
    ELSE IF BugUncappedDurableAck THEN durable
    ELSE Min(durable, ver)

-----------------------------------------------------------------------------

Init ==
    /\ currentTerm = [s \in Server |-> 0]
    /\ role = [s \in Server |-> "Follower"]
    /\ log = [s \in Server |-> <<>>]
    /\ durableLen = [s \in Server |-> 0]
    /\ commitIndex = [s \in Server |-> 0]
    /\ matchMem = [s \in Server |-> [t \in Server |-> 0]]
    /\ matchDur = [s \in Server |-> [t \in Server |-> 0]]
    /\ deleted = [s \in Server |-> {}]
    /\ abstain = [s \in Server |-> FALSE]
    /\ published = {}
    /\ confirmed = {}
    /\ delivered = {}
    /\ committed = {}
    /\ unsafeRecovery = FALSE
    /\ crashes = 0

-----------------------------------------------------------------------------
\* Elections

\* s wins the votes of quorum Q in the next term. Each voter grants its vote
\* by Raft's rules: it hasn't voted in that term yet (its term is lower) and
\* the candidate's log is at least as up to date as its own. An abstaining
\* server doesn't vote. The new leader appends a no-op, so it can commit (and
\* deliver) what earlier leaders left uncommitted.
Elect(s, Q) ==
    LET term == currentTerm[s] + 1
        logOk(v) == \/ LastTerm(log[s]) > LastTerm(log[v])
                    \/ /\ LastTerm(log[s]) = LastTerm(log[v])
                       /\ Len(log[s]) >= Len(log[v])
    IN /\ s \in Q
       /\ term <= MaxTerm
       /\ \A v \in Q : currentTerm[v] < term /\ ~abstain[v] /\ logOk(v)
       /\ currentTerm' = [v \in Server |-> IF v \in Q THEN term ELSE currentTerm[v]]
       /\ role' = [v \in Server |-> IF v = s THEN "Leader"
                                    ELSE IF v \in Q THEN "Follower" ELSE role[v]]
       /\ log' = [log EXCEPT ![s] = Append(@, [term |-> term, op |-> NoopOp])]
       /\ matchMem' = [matchMem EXCEPT ![s] = [t \in Server |-> 0]]
       /\ matchDur' = [matchDur EXCEPT ![s] = [t \in Server |-> 0]]
       /\ UNCHANGED <<durableLen, commitIndex, deleted, abstain, published,
                      confirmed, delivered, committed, unsafeRecovery, crashes>>

-----------------------------------------------------------------------------
\* The broker on the leader

LeaderAppend(s, op) ==
    log' = [log EXCEPT ![s] = Append(@, [term |-> currentTerm[s], op |-> op])]

ClientPublish(s, m) ==
    /\ role[s] = "Leader"
    /\ m \notin published
    /\ ~InLog(s, DelOp(FileOf[m]))     \* a deleted segment isn't written to
    /\ LeaderAppend(s, PubOp(m))
    /\ published' = published \cup {m}
    /\ UNCHANGED <<currentTerm, role, durableLen, commitIndex, matchMem, matchDur,
                   deleted, abstain, confirmed, delivered, committed,
                   unsafeRecovery, crashes>>

Deliver(s, m) ==
    /\ role[s] = "Leader"
    /\ <<m, currentTerm[s]>> \notin delivered
    /\ InLog(s, PubOp(m))
    /\ ~InLog(s, AckOp(m))
    /\ LET i == IndexOf(s, PubOp(m))
       IN CASE DeliveryGate = "durable"    -> i <= commitIndex[s]
            [] DeliveryGate = "replicated" -> i <= Max(commitIndex[s], ReplIndex(s))
            [] DeliveryGate = "none"       -> TRUE
    /\ delivered' = delivered \cup {<<m, currentTerm[s]>>}
    /\ UNCHANGED <<currentTerm, role, log, durableLen, commitIndex, matchMem,
                   matchDur, deleted, abstain, published, confirmed, committed,
                   unsafeRecovery, crashes>>

\* A consumer acks a message it was delivered
ConsumerAck(s, m) ==
    /\ role[s] = "Leader"
    /\ \E d \in delivered : d[1] = m
    /\ InLog(s, PubOp(m))
    /\ ~InLog(s, AckOp(m))
    /\ LeaderAppend(s, AckOp(m))
    /\ UNCHANGED <<currentTerm, role, durableLen, commitIndex, matchMem, matchDur,
                   deleted, abstain, published, confirmed, delivered, committed,
                   unsafeRecovery, crashes>>

\* A segment whose messages are all acked is deleted
ProposeDelete(s, f) ==
    LET pubs == {m \in Msg : FileOf[m] = f /\ InLog(s, PubOp(m))}
    IN /\ role[s] = "Leader"
       /\ ~InLog(s, DelOp(f))
       /\ pubs # {}
       /\ \A m \in pubs : InLog(s, AckOp(m))
       /\ LeaderAppend(s, DelOp(f))
       /\ deleted' = IF BugEagerDelete
                     THEN [deleted EXCEPT ![s] = @ \cup {f}] ELSE deleted
       /\ UNCHANGED <<currentTerm, role, durableLen, commitIndex, matchMem, matchDur,
                      abstain, published, confirmed, delivered, committed,
                      unsafeRecovery, crashes>>

-----------------------------------------------------------------------------
\* Replication

\* The leader replicates to t, at most one entry, after any index i of its
\* log: whatever its next_index logic picks is one of these. With t's ack
\* this is one step, see README.md. A follower in a later term deposes the
\* leader instead.
Replicate(s, t, i) ==
    /\ role[s] = "Leader"
    /\ t # s
    /\ i \in 0..Len(log[s])
    /\ IF currentTerm[t] > currentTerm[s]
       THEN /\ currentTerm' = [currentTerm EXCEPT ![s] = currentTerm[t]]
            /\ role' = [role EXCEPT ![s] = "Follower"]
            /\ UNCHANGED <<log, durableLen, commitIndex, matchMem, matchDur,
                           deleted, abstain>>
       ELSE
       LET entries == SubSeq(log[s], i + 1, Min(i + 1, Len(log[s])))
           prevTerm == IF i = 0 THEN 0 ELSE log[s][i].term
           logOk == i = 0 \/ (i <= Len(log[t]) /\ log[t][i].term = prevTerm)
           entry == entries[1]
           \* Raft only truncates on a conflict
           keep == \/ entries = <<>>
                   \/ Len(log[t]) > i /\ log[t][i + 1].term = entry.term
           newLog == IF keep THEN log[t] ELSE Append(SubSeq(log[t], 1, i), entry)
           newDurable == IF keep THEN durableLen[t] ELSE Min(durableLen[t], i)
           ver == i + Len(entries)
           caughtUp == logOk /\ ver = Len(log[s])
       IN /\ currentTerm' = [currentTerm EXCEPT ![t] = currentTerm[s]]
          /\ role' = [role EXCEPT ![t] = "Follower"]
          /\ abstain' = [abstain EXCEPT ![t] =
                            IF BugStopAbstainOnContact \/ caughtUp THEN FALSE ELSE @]
          /\ IF ~logOk
             THEN UNCHANGED <<log, durableLen, commitIndex, matchMem, matchDur, deleted>>
             ELSE /\ log' = [log EXCEPT ![t] = newLog]
                  /\ durableLen' = [durableLen EXCEPT ![t] = newDurable]
                  /\ commitIndex' = [commitIndex EXCEPT ![t] =
                                        Max(@, Min(commitIndex[s], ver))]
                  /\ deleted' = IF BugEagerDelete /\ ~keep /\ entry.op.type = "del"
                                THEN [deleted EXCEPT ![t] = @ \cup {entry.op.arg}]
                                ELSE deleted
                  \* Only the "replicated" gate reads it
                  /\ matchMem' = IF DeliveryGate = "replicated"
                                 THEN [matchMem EXCEPT ![s][t] = Max(@, ver)]
                                 ELSE matchMem
                  /\ matchDur' = [matchDur EXCEPT ![s][t] =
                                     Max(@, DurableAck(newDurable, ver))]
    /\ UNCHANGED <<published, confirmed, delivered, committed, unsafeRecovery, crashes>>

\* Group commit. A follower's ack of it rides on its next Replicate.
Fsync(s) ==
    /\ durableLen[s] < Len(log[s])
    /\ durableLen' = [durableLen EXCEPT ![s] = Len(log[s])]
    /\ UNCHANGED <<currentTerm, role, log, commitIndex, matchMem, matchDur, deleted,
                   abstain, published, confirmed, delivered, committed,
                   unsafeRecovery, crashes>>

AdvanceCommit(s) ==
    LET n == QuorumIndex(s, matchDur[s], durableLen[s])
    IN /\ role[s] = "Leader"
       /\ n > commitIndex[s]
       /\ commitIndex' = [commitIndex EXCEPT ![s] = n]
       /\ committed' = committed \cup
            {<<i, log[s][i], currentTerm[s]>> :
                i \in {j \in 1..n : ~\E c \in committed : c[1] = j /\ c[2] = log[s][j]}}
       \* The publishers get their confirms: the leader confirms what it was
       \* published, i.e. what it appended in its term
       /\ confirmed' = confirmed \cup
            {<<log[s][i].op.arg, currentTerm[s]>> :
                i \in {j \in 1..n : log[s][j].op.type = "pub" /\ log[s][j].term = currentTerm[s]}}
       /\ UNCHANGED <<currentTerm, role, log, durableLen, matchMem, matchDur, deleted,
                      abstain, published, delivered, unsafeRecovery, crashes>>

\* A delete is carried out once committed and fsynced here, so neither a
\* truncation nor a power loss can take the entry back
ApplyDelete(s, f) ==
    /\ ~BugEagerDelete
    /\ f \notin deleted[s]
    /\ InLog(s, DelOp(f))
    /\ IndexOf(s, DelOp(f)) <= Min(commitIndex[s], durableLen[s])
    /\ deleted' = [deleted EXCEPT ![s] = @ \cup {f}]
    /\ UNCHANGED <<currentTerm, role, log, durableLen, commitIndex, matchMem, matchDur,
                   abstain, published, confirmed, delivered, committed,
                   unsafeRecovery, crashes>>

-----------------------------------------------------------------------------
\* Failures

ResetVolatile(s) ==
    /\ role' = [role EXCEPT ![s] = "Follower"]
    /\ commitIndex' = [commitIndex EXCEPT ![s] = 0]
    /\ matchMem' = [matchMem EXCEPT ![s] = [t \in Server |-> 0]]
    /\ matchDur' = [matchDur EXCEPT ![s] = [t \in Server |-> 0]]
    /\ crashes' = crashes + 1

\* The process restarts but the machine doesn't: the page cache kept what it
\* wrote, so it lost nothing (LavinMQ can tell by the boot id) and votes.
Restart(s) ==
    /\ crashes < MaxCrashes
    /\ ResetVolatile(s)
    /\ UNCHANGED <<currentTerm, log, durableLen, deleted, abstain, published,
                   confirmed, delivered, committed, unsafeRecovery>>

\* The machine loses power: what wasn't fsynced is gone
PowerLoss(s) ==
    /\ crashes < MaxCrashes
    /\ ResetVolatile(s)
    /\ log' = [log EXCEPT ![s] = SubSeq(@, 1, durableLen[s])]
    /\ abstain' = [abstain EXCEPT ![s] = AbstainAfterPowerLoss]
    /\ UNCHANGED <<currentTerm, durableLen, deleted, published, confirmed, delivered,
                   committed, unsafeRecovery>>

\* With no majority left that may vote, abstaining would keep the cluster
\* without a leader for good. Giving up may lose what was only delivered.
GiveUpAbstaining(s) ==
    /\ abstain[s]
    /\ {t \in Server : ~abstain[t]} \notin Quorum
    /\ abstain' = [abstain EXCEPT ![s] = FALSE]
    /\ unsafeRecovery' = TRUE
    /\ UNCHANGED <<currentTerm, role, log, durableLen, commitIndex, matchMem, matchDur,
                   deleted, published, confirmed, delivered, committed, crashes>>

-----------------------------------------------------------------------------

Next ==
    \E s \in Server :
        \/ \E Q \in Quorum : Elect(s, Q)
        \/ \E t \in Server, i \in 0..MaxLogLen : Replicate(s, t, i)
        \/ Fsync(s)
        \/ AdvanceCommit(s)
        \/ Restart(s)
        \/ PowerLoss(s)
        \/ GiveUpAbstaining(s)
        \/ \E m \in Msg : ClientPublish(s, m) \/ Deliver(s, m) \/ ConsumerAck(s, m)
        \/ \E f \in File : ProposeDelete(s, f) \/ ApplyDelete(s, f)

Spec == Init /\ [][Next]_vars

\* Bound for model checking
StateConstraint == \A s \in Server : Len(log[s]) <= MaxLogLen

-----------------------------------------------------------------------------
\* Invariants

Leaders == {s \in Server : role[s] = "Leader"}

\* At most one leader per term
ElectionSafety ==
    \A s, t \in Leaders : currentTerm[s] = currentTerm[t] => s = t

\* Committed entries are never taken back: every leader of a later term than
\* the one an entry was committed in has it
LeaderCompleteness ==
    \A c \in committed, s \in Leaders :
        currentTerm[s] > c[3] =>
            /\ c[1] <= Len(log[s])
            /\ log[s][c[1]] = c[2]

\* A confirmed message is with every leader of that term or later, and still
\* on its disk unless a consumer acked it. A deposed leader that doesn't know
\* yet may lack it, it can't commit or confirm anything anymore.
ConfirmedNotLost ==
    \A c \in confirmed, s \in Leaders :
        currentTerm[s] >= c[2] =>
            /\ InLog(s, PubOp(c[1]))
            /\ InLog(s, AckOp(c[1])) \/ FileOf[c[1]] \notin deleted[s]

\* A delivered message doesn't disappear: every leader of that term or later
\* has it. Only a majority losing power at once (and then electing a leader
\* anyway) may break this.
NoPhantomDelivery ==
    unsafeRecovery \/
        \A d \in delivered, s \in Leaders :
            currentTerm[s] >= d[2] => InLog(s, PubOp(d[1]))

\* A file is only deleted for a committed delete
NoPrematureDelete ==
    \A s \in Server, f \in File :
        f \in deleted[s] => \E c \in committed : c[2].op = DelOp(f)

TypeOK ==
    /\ role \in [Server -> {"Follower", "Leader"}]
    /\ \A s \in Server : durableLen[s] <= Len(log[s])

=============================================================================

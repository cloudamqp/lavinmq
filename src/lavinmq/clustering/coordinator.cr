module LavinMQ::Clustering
  # Abstracts away how a cluster elects a leader, stores the in-sync replica
  # (ISR) set, advertises the leader's URI and shares the replication secret.
  #
  # `Clustering::Server` (the leader side) only needs `update_isr` and
  # `password`, so those are the only abstract methods — that keeps the
  # Server-only test doubles (NullCoordinator, SpyCoordinator) tiny. The
  # lifecycle/election methods below are used solely by `Clustering::Controller`.
  # They are *not* abstract (so the Server-only doubles don't have to implement
  # them) but their base implementations raise `NotImplementedError` rather than
  # silently no-op: a real backend that forgets to override one fails loudly the
  # first time the Controller calls it, instead of, say, never discovering a
  # leader and silently never replicating. The two genuinely-optional hooks
  # (`start`, `release`) keep a no-op default; `membership_lost`/`isr` keep a
  # safe-value default. The real backends (EtcdCoordinator, RaftCoordinator)
  # override everything they use.
  #
  # All methods are safe to call from any thread/fiber.
  abstract class Coordinator
    # Replace the ISR set wholesale with the given node ids. Must only succeed
    # while this node is still the leader, and must be durable before it
    # returns (Server treats a successful return as "the ISR write is
    # committed"). Raises if leadership has been lost so Server#flush_isr
    # retries.
    abstract def update_isr(synced_node_ids : Set(Int32)) : Nil

    # The cluster's shared replication secret, used to authenticate followers.
    abstract def password : String

    # Join the cluster / acquire the membership handle. Must be called before
    # any other lifecycle method. Raises `IdConflict` if this node's id is
    # already taken by another live node.
    def start : Nil
    end

    # Block until THIS node is the cluster leader.
    def await_leadership : Nil
      raise NotImplementedError.new("await_leadership")
    end

    # Block until this node's leadership is lost, then return. The caller
    # (Controller) exits the process afterwards.
    def await_leadership_lost : Nil
      raise NotImplementedError.new("await_leadership_lost")
    end

    # A channel that is closed when this node's membership/leadership is lost,
    # so a caller waiting for something else (e.g. to enter the ISR) can abort.
    # The base implementation never closes.
    def membership_lost : Channel(Nil)
      Channel(Nil).new
    end

    # Voluntarily give up leadership/membership immediately (graceful shutdown
    # or stepping down because not in the ISR). Idempotent, never raises.
    def release : Nil
    end

    # The current in-sync replica set, or nil if none has been recorded yet (a
    # fresh cluster).
    def isr : Set(Int32)?
      nil
    end

    # Yield the parsed ISR set on every change (nil when no ISR is recorded),
    # until the block breaks. Blocks like a watch.
    def watch_isr(& : Set(Int32)? -> Nil) : Nil
      raise NotImplementedError.new("watch_isr")
    end

    # Yield the current leader's advertised URI on every change (nil when there
    # is no leader), until the block breaks. Used by the follower monitor to
    # know where to replicate from.
    def watch_leader_uri(& : String? -> Nil) : Nil
      raise NotImplementedError.new("watch_leader_uri")
    end

    # Publish this node's advertised URI as the current leader's URI. Called by
    # the Controller right after `await_leadership` returns.
    def publish_leader_uri(advertised_uri : String) : Nil
      raise NotImplementedError.new("publish_leader_uri")
    end

    # Raised by `start`/`await_leadership` when this node's id is already in use
    # by another live node in the cluster.
    class IdConflict < Exception; end
  end
end

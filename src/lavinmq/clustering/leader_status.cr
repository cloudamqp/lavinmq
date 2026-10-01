module LavinMQ::Clustering
  # This node's view of the cluster leadership, broadcast to subscribers of
  # the status socket. `ready` means this node is the leader and accepting
  # client connections, it's what a router should act on.
  class LeaderStatus
    record Snapshot, ready : Bool, leader : Bool, term : Int64, leader_uri : String?, seq : Int64 do
      def to_s(io : IO) : Nil
        io << "ready=" << (ready ? 1 : 0)
        io << " leader=" << (leader ? 1 : 0)
        io << " term=" << term
        io << " seq=" << seq
        io << " leader_uri=" << leader_uri
      end
    end

    @lock = Mutex.new
    @snapshot = Snapshot.new(false, false, 0i64, nil, 0i64)
    @subscribers = Array(Channel(Snapshot)).new

    def snapshot : Snapshot
      @lock.synchronize { @snapshot }
    end

    def ready=(ready : Bool) : Nil
      change(&.copy_with(ready: ready))
    end

    def raft_state(leader : Bool, term : Int64, leader_uri : String?) : Nil
      change(&.copy_with(leader: leader, term: term, leader_uri: leader_uri))
    end

    # The returned channel starts out with the current snapshot. A subscriber
    # that falls behind only gets the latest one.
    def subscribe : Channel(Snapshot)
      ch = Channel(Snapshot).new(1)
      @lock.synchronize do
        ch.send @snapshot
        @subscribers << ch
      end
      ch
    end

    def unsubscribe(ch : Channel(Snapshot)) : Nil
      @lock.synchronize { @subscribers.delete(ch) }
      ch.close
    end

    def subscriber_count : Int32
      @lock.synchronize { @subscribers.size }
    end

    # Closes all subscriptions, already queued snapshots are still delivered.
    def close : Nil
      @lock.synchronize do
        @subscribers.each &.close
        @subscribers.clear
      end
    end

    private def change(& : Snapshot -> Snapshot) : Nil
      @lock.synchronize do
        s = yield @snapshot
        next if s == @snapshot
        @snapshot = s = s.copy_with(seq: s.seq + 1)
        @subscribers.each { |ch| replace(ch, s) }
      end
    end

    # Only `change` sends, under the lock, so after dropping the stale
    # snapshot the send can't block.
    private def replace(ch : Channel(Snapshot), s : Snapshot) : Nil
      loop do
        select
        when ch.send(s)
          return
        else
          select
          when ch.receive?
          else
          end
        end
      end
    rescue Channel::ClosedError
    end
  end
end

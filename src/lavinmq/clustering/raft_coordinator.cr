require "raft"
require "./coordinator"
require "../config"
require "../bool_channel"

module LavinMQ::Clustering
  # A Coordinator backed by an embedded Raft group (the `raft` shard) instead of
  # an external etcd. The replicated state machine stores the ISR set and the
  # current leader's advertised URI; the replication secret is read from a local
  # file (`<data_dir>/.replication_secret`), never the Raft log.
  #
  # Concurrency: raft.cr's `Raft::Node` is single-threaded — only the driver
  # fiber may call step/tick/propose/take_messages. Every public method that
  # needs the node marshals a closure onto that fiber through `@ops`. Reads of
  # ISR/leader state bypass the node entirely and read the state machine's own
  # mutex-guarded copy.
  class RaftCoordinator < Coordinator
    Log = LavinMQ::Log.for "clustering.raft_coordinator"

    GROUP_ID = 0_u64

    @node : Raft::Node(RaftCommand)
    @transport : Raft::Transport
    @ops = Channel(Proc(Raft::Node(RaftCommand), Nil)).new
    @is_leader = BoolChannel.new(false)
    @closing = Atomic(Bool).new(false)
    @tick_interval : Time::Span
    # How long await_leadership_lost rides out a momentary step-down before
    # treating leadership as truly lost (≈ one election cycle).
    @election_grace : Time::Span
    # Closed when this node's coordinator is shutting down, so a follower parked
    # in wait_to_be_insync can abort.
    @membership_lost = Channel(Nil).new
    # Closed by release so a leader parked in await_leadership_lost wakes up and
    # lets Controller#run finish its cleanup on a graceful/programmatic stop
    # (raft has no lease whose expiry would otherwise wake it).
    @released = Channel(Nil).new
    # Set while this node should (re)publish its advertised URI as the leader's;
    # the driver loop proposes it until accepted, and clears it. Re-armed on
    # every leadership acquisition (on_role_change).
    @want_publish_uri = Atomic(Bool).new(false)
    # Pending "wait until index N (proposed at term T) is committed+applied"
    # requests, as {index, term, channel}. Only touched on the driver fiber
    # (added by an op, resolved/failed by the loop), so no lock.
    @commit_waiters = [] of {UInt64, UInt64, Channel(Symbol)}

    def initialize(@config : Config, @id : Int32, @advertised_uri : String, transport : Raft::Transport? = nil)
      raft_dir = File.join(@config.data_dir, "raft")
      Dir.mkdir_p raft_dir

      peers = parse_peers(@config.clustering_raft_peers)
      peer_ids = peers.map(&.[0]).reject { |pid| pid == @id.to_u64 }

      cfg = Raft::Config.new
      cfg.data_dir = raft_dir

      @sm = SM.new
      @node = Raft::Node(RaftCommand).new(
        id: @id.to_u64,
        peers: peer_ids,
        config: cfg,
        state_machine: @sm,
        group_id: GROUP_ID,
        address: "#{@config.clustering_raft_bind}:#{@config.clustering_raft_port}",
      )
      @tick_interval = cfg.tick_interval
      @election_grace = cfg.tick_interval * (cfg.election_timeout_max_ticks + cfg.heartbeat_ticks)

      @transport = transport || Raft::TCPTransport.new(
        listen_address: @config.clustering_raft_bind,
        listen_port: @config.clustering_raft_port,
        data_dir: raft_dir,
      )
      peers.each do |pid, host, port|
        @transport.register_peer(pid, host, port) unless pid == @id.to_u64
      end

      # on_role_change runs on the driver fiber (or, for the bootstrap-leader
      # transition, on the fiber calling start before the loop spawns) — keep it
      # non-blocking and don't re-enter the node here.
      @node.on_role_change do |_old, new_role|
        @is_leader.set(new_role.leader?)
        if new_role.leader?
          @want_publish_uri.set(true) # (re)advertise our URI once we drive the loop
        else
          # We are no longer leader: any in-flight ISR write can't be guaranteed,
          # so fail every pending waiter (driver fiber only, see @commit_waiters).
          @want_publish_uri.set(false)
          fail_commit_waiters
        end
      end
    end

    def start : Nil
      @transport.register_channel(GROUP_ID, @node.inbox)
      @transport.start
      # A node with no configured peers can't hold an election — bootstrap it as
      # a single-node cluster. On restart the persisted config makes bootstrap a
      # no-op and the lone voter re-elects itself.
      @node.bootstrap if @node.peers.empty?
      spawn(run_loop, name: "raft-coordinator")
    end

    private def run_loop
      node = @node
      transport = @transport
      loop do
        select
        when msg = node.inbox.receive
          node.step(msg)
        when op = @ops.receive
          op.call(node)
        when timeout(@tick_interval)
          node.tick
        end
        drain_outbox(node, transport)
        resolve_commit_waiters(node)
        maybe_publish_leader_uri(node)
        if @closing.get
          drain_outbox(node, transport) # flush a pending leadership transfer
          break
        end
      rescue Channel::ClosedError
        break
      end
      # Stop receiving ops and unblock anything still waiting: closing @ops makes
      # a sender blocked on the unbuffered rendezvous raise ClosedError (which
      # every op-enqueuing method rescues) instead of hanging forever; failing
      # the commit waiters releases any update_isr parked on its result; closing
      # membership_lost aborts a follower parked in wait_to_be_insync.
      @ops.close
      fail_commit_waiters
      @membership_lost.close rescue nil
      node.close
      transport.stop
    end

    private def drain_outbox(node, transport) : Nil
      node.take_messages.each do |target_id, msg|
        transport.outbox.send({target_id, msg})
      end
    end

    private def resolve_commit_waiters(node) : Nil
      return if @commit_waiters.empty?
      applied = node.last_applied
      @commit_waiters.reject! do |index, term, ch|
        next false if applied < index
        # The entry at `index` has been applied; confirm it's still OUR entry. A
        # new leader can overwrite an uncommitted index after we stepped down, so
        # only a matching term proves the ISR write we proposed actually carried.
        committed = node.log.term_at(index) == term
        ch.send(committed ? :committed : :lost) rescue nil
        true
      end
    end

    # Release every pending ISR waiter as lost (no longer leader / shutting
    # down). Driver-fiber only.
    private def fail_commit_waiters : Nil
      return if @commit_waiters.empty?
      @commit_waiters.each { |_index, _term, ch| ch.send(:lost) rescue nil }
      @commit_waiters.clear
    end

    # Publish our advertised URI while we're leader, retrying each loop iteration
    # until a proposal is accepted, then clear the flag. Coupling publication to
    # leadership (re-armed by on_role_change) closes the window where a node
    # serves as leader but no leader URI was ever committed.
    private def maybe_publish_leader_uri(node) : Nil
      return if @closing.get
      return unless @want_publish_uri.get
      return unless node.role.leader?
      @want_publish_uri.set(false) if node.propose(RaftCommand.for_leader(@advertised_uri))
    end

    def await_leadership : Nil
      @is_leader.when_true.receive
    end

    def await_leadership_lost : Nil
      # Raft can briefly step a leader down (e.g. a higher-term heartbeat) and
      # then re-win; treat leadership as lost only if it isn't regained within a
      # grace window, so a momentary flap doesn't kill the process. This is safe
      # because update_isr independently gates durability on node.role.leader?,
      # so nothing is acknowledged while we're transiently not the leader.
      # `release` (graceful stop) closes @released to return immediately, since
      # there's no lease expiry to wake us as in the etcd backend.
      loop do
        select
        when @is_leader.when_false.receive
          # leadership lost — possibly a transient flap, fall through to grace
        when @released.receive?
          return
        end
        select
        when @is_leader.when_true.receive
          next # regained leadership, keep serving
        when @released.receive?
          return
        when timeout(@election_grace)
          return # durably lost
        end
      end
    end

    def membership_lost : Channel(Nil)
      @membership_lost
    end

    def release : Nil
      return if @closing.swap(true)
      @released.close rescue nil # wake await_leadership_lost so the stop completes
      # Best-effort fast failover: hand leadership to a caught-up voter before
      # tearing down. The loop flushes the resulting TimeoutNow then exits.
      @ops.send(->(node : Raft::Node(RaftCommand)) do
        if node.role.leader?
          if target = node.voters.find { |p| p.id != node.id }
            node.transfer_leadership(to: target.id)
          end
        end
        nil
      end)
    rescue Channel::ClosedError
    end

    def update_isr(synced_node_ids : Set(Int32)) : Nil
      # The driver loop stops consuming @ops once closing, so don't enqueue (it
      # would block forever). Server#flush_isr treats this like a lost-leadership
      # and stops once the process exits.
      raise StaleLeadership.new("Raft node closing") if @closing.get
      result = Channel(Symbol).new(1)
      begin
        @ops.send(->(node : Raft::Node(RaftCommand)) do
          if !node.role.leader? || !node.propose(RaftCommand.for_isr(synced_node_ids))
            result.send(:not_leader)
          else
            index = node.log.last_index
            term = node.current_term
            if node.last_applied >= index && node.log.term_at(index) == term
              result.send(:committed)
            else
              @commit_waiters << {index, term, result}
            end
          end
          nil
        end)
      rescue Channel::ClosedError
        raise StaleLeadership.new("Raft node closed")
      end

      # Block until the ISR write is committed+applied (durable) before
      # returning, since Server treats a successful return as durable. Anything
      # other than :committed (not leader, or the entry was discarded after a
      # leadership change) raises so Server#flush_isr retries.
      select
      when r = result.receive
        raise StaleLeadership.new("ISR write not committed (#{r})") unless r == :committed
      when @is_leader.when_false.receive
        raise StaleLeadership.new("Lost Raft leadership before ISR committed")
      end
    end

    def isr : Set(Int32)?
      @sm.isr
    end

    def watch_isr(&) : Nil
      bell = @sm.subscribe_isr
      begin
        loop do
          bell.receive
          yield @sm.isr
        end
      ensure
        @sm.unsubscribe_isr(bell)
      end
    end

    def watch_leader_uri(&) : Nil
      bell = @sm.subscribe_leader_uri
      begin
        loop do
          bell.receive
          yield current_leader_uri
        end
      ensure
        @sm.unsubscribe_leader_uri(bell)
      end
    end

    # The leader URI to report to watchers. A persisted SetLeader survives a
    # restart, and subscribe delivers it immediately; if it still points at us
    # but we're no longer the leader it's stale (we led before restarting), so
    # report no leader until a fresh election publishes one. Otherwise
    # follow_leader would see our own URI, fail the 1s self-URI check and abort
    # as a duplicate. Mirrors etcd, whose lease-bound election value simply
    # vanishes when the old leader dies.
    private def current_leader_uri : String?
      uri = @sm.leader_uri
      return nil if uri == @advertised_uri && !@is_leader.value
      uri
    end

    def publish_leader_uri(advertised_uri : String) : Nil
      # The advertised URI is fixed at construction (@advertised_uri), so just
      # arm the flag; the driver loop publishes it while we're leader and retries
      # until a proposal is accepted. on_role_change also arms it on every
      # leadership acquisition, so the URI is published even without this call —
      # there is no window where we lead but never advertise.
      @want_publish_uri.set(true) unless @closing.get
    end

    def password : String
      path = File.join(@config.data_dir, ".replication_secret")
      File.read(path).strip
    rescue File::NotFoundError
      Log.fatal { "Replication secret file missing: #{path}. Create it (a shared secret) and copy it to every node." }
      exit 3
    end

    # Parse "1@host1:5680,2@host2:5680" into [{1, "host1", 5680}, ...]. IPv6
    # hosts must be bracketed: "3@[::1]:5680". Raises ArgumentError with the
    # offending entry on malformed input rather than crashing with an opaque
    # error deep in construction.
    private def parse_peers(raw : String) : Array({UInt64, String, Int32})
      peers = [] of {UInt64, String, Int32}
      raw.split(',') do |entry|
        entry = entry.strip
        next if entry.empty?
        id_part, sep, addr = entry.partition('@')
        raise ArgumentError.new("Invalid raft peer #{entry.inspect}, expected id@host:port") if sep.empty? || addr.empty?
        id = id_part.to_u64? || raise ArgumentError.new("Invalid raft peer id in #{entry.inspect}")
        host, port = parse_host_port(addr, entry)
        peers << {id, host, port}
      end
      peers
    end

    private def parse_host_port(addr : String, entry : String) : {String, Int32}
      if addr.starts_with?('[') # bracketed IPv6: [::1]:5680
        close = addr.index(']') || raise ArgumentError.new("Unterminated IPv6 address in raft peer #{entry.inspect}")
        host = addr[1...close]
        rest = addr[(close + 1)..]
        raise ArgumentError.new("Missing port in raft peer #{entry.inspect}") unless rest.starts_with?(':')
        port_part = rest[1..]
      else
        host, sep, port_part = addr.rpartition(':')
        raise ArgumentError.new("Missing port in raft peer #{entry.inspect}") if sep.empty? || host.empty?
      end
      port = port_part.to_i? || raise ArgumentError.new("Invalid port in raft peer #{entry.inspect}")
      {host, port}
    end

    # The replicated command: replace the ISR wholesale, or set the leader's
    # advertised URI. Both are idempotent full replacements.
    struct RaftCommand
      enum Kind : UInt8
        SetISR    = 0
        SetLeader = 1
      end

      getter kind : Kind
      getter isr : Set(Int32)
      getter leader_uri : String

      def initialize(@kind, @isr, @leader_uri)
      end

      def self.for_isr(isr : Set(Int32)) : self
        new(Kind::SetISR, isr, "")
      end

      def self.for_leader(leader_uri : String) : self
        new(Kind::SetLeader, Set(Int32).new, leader_uri)
      end

      def bytesize : Int32
        case kind
        in .set_isr?    then 1 + 4 + 4 * @isr.size
        in .set_leader? then 1 + 2 + @leader_uri.bytesize
        end
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian)
        io.write_byte(@kind.value)
        case kind
        in .set_isr?
          io.write_bytes(@isr.size.to_u32, format)
          @isr.each { |id| io.write_bytes(id, format) }
        in .set_leader?
          io.write_bytes(@leader_uri.bytesize.to_u16, format)
          io.write(@leader_uri.to_slice)
        end
      end

      def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
        kind = Kind.new(io.read_bytes(UInt8, format))
        case kind
        in .set_isr?
          count = io.read_bytes(UInt32, format)
          isr = Set(Int32).new(count)
          count.times { isr << io.read_bytes(Int32, format) }
          for_isr(isr)
        in .set_leader?
          len = io.read_bytes(UInt16, format)
          for_leader(io.read_string(len))
        end
      end
    end

    # The replicated state machine. apply runs on the Raft driver fiber and must
    # not block; subscriber notification is a non-blocking doorbell ("something
    # changed, go read the latest"), so updates can't be lost and a slow watcher
    # can't stall the Raft loop.
    class SM < Raft::StateMachine(RaftCommand)
      @lock = Mutex.new
      @isr : Set(Int32)? = nil # nil until the first SetISR (fresh cluster)
      @leader_uri : String? = nil
      @isr_bells = [] of Channel(Nil)
      @uri_bells = [] of Channel(Nil)

      def apply(entry : RaftCommand)
        case entry.kind
        in .set_isr?
          @lock.synchronize { @isr = entry.isr }
          ring(@isr_bells)
        in .set_leader?
          @lock.synchronize { @leader_uri = entry.leader_uri }
          ring(@uri_bells)
        end
      end

      def isr : Set(Int32)?
        @lock.synchronize { @isr.dup }
      end

      def leader_uri : String?
        @lock.synchronize { @leader_uri }
      end

      def subscribe_isr : Channel(Nil)
        bell = Channel(Nil).new(1)
        @lock.synchronize { @isr_bells << bell }
        bell.send(nil) rescue nil # deliver current value immediately
        bell
      end

      def unsubscribe_isr(bell : Channel(Nil)) : Nil
        @lock.synchronize { @isr_bells.delete(bell) }
      end

      def subscribe_leader_uri : Channel(Nil)
        bell = Channel(Nil).new(1)
        @lock.synchronize { @uri_bells << bell }
        bell.send(nil) rescue nil
        bell
      end

      def unsubscribe_leader_uri(bell : Channel(Nil)) : Nil
        @lock.synchronize { @uri_bells.delete(bell) }
      end

      private def ring(bells : Array(Channel(Nil))) : Nil
        @lock.synchronize do
          bells.each do |bell|
            select
            when bell.send(nil)
            else # already signalled; watcher will read the latest value
            end
          end
        end
      end

      def snapshot(io : IO)
        @lock.synchronize do
          uri = @leader_uri || ""
          io.write_bytes(uri.bytesize.to_u16, IO::ByteFormat::LittleEndian)
          io.write(uri.to_slice)
          if isr = @isr
            io.write_byte(1u8)
            io.write_bytes(isr.size.to_u32, IO::ByteFormat::LittleEndian)
            isr.each { |id| io.write_bytes(id, IO::ByteFormat::LittleEndian) }
          else
            io.write_byte(0u8)
          end
        end
      end

      def restore(io : IO)
        len = io.read_bytes(UInt16, IO::ByteFormat::LittleEndian)
        uri = io.read_string(len)
        has_isr = io.read_bytes(UInt8, IO::ByteFormat::LittleEndian)
        isr = nil
        if has_isr == 1
          count = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
          isr = Set(Int32).new(count)
          count.times { isr << io.read_bytes(Int32, IO::ByteFormat::LittleEndian) }
        end
        @lock.synchronize do
          @leader_uri = uri.empty? ? nil : uri
          @isr = isr
        end
        ring(@isr_bells)
        ring(@uri_bells)
      end
    end

    class StaleLeadership < Exception; end
  end
end

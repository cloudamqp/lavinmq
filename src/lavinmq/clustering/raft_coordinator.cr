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
    # Pending "wait until index N is committed+applied" requests. Only touched
    # on the driver fiber (added by an op, resolved by the loop), so no lock.
    @commit_waiters = [] of {UInt64, Channel(Symbol)}

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

      @transport = transport || Raft::TCPTransport.new(
        listen_address: @config.clustering_raft_bind,
        listen_port: @config.clustering_raft_port,
        data_dir: raft_dir,
      )
      peers.each do |pid, host, port|
        @transport.register_peer(pid, host, port) unless pid == @id.to_u64
      end

      # on_role_change runs on the driver fiber — keep it non-blocking.
      @node.on_role_change do |_old, new_role|
        @is_leader.set(new_role.leader?)
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
        resolve_commit_waiters(node.last_applied)
        if @closing.get
          drain_outbox(node, transport) # flush a pending leadership transfer
          break
        end
      rescue Channel::ClosedError
        break
      end
      node.close
      transport.stop
    end

    private def drain_outbox(node, transport) : Nil
      node.take_messages.each do |target_id, msg|
        transport.outbox.send({target_id, msg})
      end
    end

    private def resolve_commit_waiters(applied : UInt64) : Nil
      @commit_waiters.reject! do |index, ch|
        if applied >= index
          ch.send(:committed) rescue nil
          true
        else
          false
        end
      end
    end

    def await_leadership : Nil
      @is_leader.when_true.receive
    end

    def await_leadership_lost : Nil
      @is_leader.when_false.receive
    end

    def release : Nil
      return if @closing.swap(true)
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
            if node.last_applied >= index
              result.send(:committed)
            else
              @commit_waiters << {index, result}
            end
          end
          nil
        end)
      rescue Channel::ClosedError
        raise StaleLeadership.new("Raft node closed")
      end

      # Block until the ISR write is committed+applied (durable) before
      # returning, since Server treats a successful return as durable. If
      # leadership is lost first, raise so Server#flush_isr retries.
      select
      when r = result.receive
        raise StaleLeadership.new("Not the Raft leader") if r == :not_leader
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
          yield @sm.leader_uri
        end
      ensure
        @sm.unsubscribe_leader_uri(bell)
      end
    end

    def publish_leader_uri(advertised_uri : String) : Nil
      return if @closing.get
      @ops.send(->(node : Raft::Node(RaftCommand)) do
        node.propose(RaftCommand.for_leader(@id, advertised_uri))
        nil
      end)
    rescue Channel::ClosedError
    end

    def password : String
      path = File.join(@config.data_dir, ".replication_secret")
      File.read(path).strip
    rescue File::NotFoundError
      Log.fatal { "Replication secret file missing: #{path}. Create it (a shared secret) and copy it to every node." }
      exit 3
    end

    # Parse "1@host1:5680,2@host2:5680" into [{1, "host1", 5680}, ...].
    private def parse_peers(raw : String) : Array({UInt64, String, Int32})
      peers = [] of {UInt64, String, Int32}
      raw.split(',') do |entry|
        entry = entry.strip
        next if entry.empty?
        id_part, _, addr = entry.partition('@')
        host, _, port = addr.rpartition(':')
        peers << {id_part.to_u64, host, port.to_i}
      end
      peers
    end

    # The replicated command: replace the ISR wholesale, or set the leader id +
    # advertised URI. Both are idempotent full replacements.
    struct RaftCommand
      enum Kind : UInt8
        SetISR    = 0
        SetLeader = 1
      end

      getter kind : Kind
      getter isr : Set(Int32)
      getter leader_id : Int32
      getter leader_uri : String

      def initialize(@kind, @isr, @leader_id, @leader_uri)
      end

      def self.for_isr(isr : Set(Int32)) : self
        new(Kind::SetISR, isr, 0, "")
      end

      def self.for_leader(leader_id : Int32, leader_uri : String) : self
        new(Kind::SetLeader, Set(Int32).new, leader_id, leader_uri)
      end

      def bytesize : Int32
        case kind
        in .set_isr?    then 1 + 4 + 4 * @isr.size
        in .set_leader? then 1 + 4 + 2 + @leader_uri.bytesize
        end
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian)
        io.write_bytes(@kind.value, format)
        case kind
        in .set_isr?
          io.write_bytes(@isr.size.to_u32, format)
          @isr.each { |id| io.write_bytes(id, format) }
        in .set_leader?
          io.write_bytes(@leader_id, format)
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
          leader_id = io.read_bytes(Int32, format)
          len = io.read_bytes(UInt16, format)
          buf = Bytes.new(len)
          io.read_fully(buf)
          for_leader(leader_id, String.new(buf))
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
      @leader_id = 0
      @leader_uri : String? = nil
      @isr_bells = [] of Channel(Nil)
      @uri_bells = [] of Channel(Nil)

      def apply(entry : RaftCommand)
        case entry.kind
        in .set_isr?
          @lock.synchronize { @isr = entry.isr }
          ring(@isr_bells)
        in .set_leader?
          @lock.synchronize { @leader_id = entry.leader_id; @leader_uri = entry.leader_uri }
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
          io.write_bytes(@leader_id, IO::ByteFormat::LittleEndian)
          uri = @leader_uri || ""
          io.write_bytes(uri.bytesize.to_u16, IO::ByteFormat::LittleEndian)
          io.write(uri.to_slice)
          if isr = @isr
            io.write_bytes(1u8, IO::ByteFormat::LittleEndian)
            io.write_bytes(isr.size.to_u32, IO::ByteFormat::LittleEndian)
            isr.each { |id| io.write_bytes(id, IO::ByteFormat::LittleEndian) }
          else
            io.write_bytes(0u8, IO::ByteFormat::LittleEndian)
          end
        end
      end

      def restore(io : IO)
        leader_id = io.read_bytes(Int32, IO::ByteFormat::LittleEndian)
        len = io.read_bytes(UInt16, IO::ByteFormat::LittleEndian)
        buf = Bytes.new(len)
        io.read_fully(buf)
        uri = String.new(buf)
        has_isr = io.read_bytes(UInt8, IO::ByteFormat::LittleEndian)
        isr = nil
        if has_isr == 1
          count = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
          isr = Set(Int32).new(count)
          count.times { isr << io.read_bytes(Int32, IO::ByteFormat::LittleEndian) }
        end
        @lock.synchronize do
          @leader_id = leader_id
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

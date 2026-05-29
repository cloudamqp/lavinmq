require "raft"
require "./cluster_command"
require "./cluster_state"

module LavinMQ::Raft
  class ClusterStateMachine < ::Raft::StateMachine(ClusterCommand)
    SNAPSHOT_VERSION = 1_u8

    # The Atomic ClusterState IS the canonical state. There are no local
    # @secret / @isr fields to keep in sync — apply reads the current snapshot,
    # constructs a new one, publishes it.
    @snapshot = Atomic(ClusterState).new(ClusterState::EMPTY)

    # Cross-thread-safe accessor: atomic load of the latest published snapshot.
    # Use this when you need multiple fields consistent.
    def state : ClusterState
      @snapshot.get(:acquire)
    end

    # Convenience shortcuts; each is one atomic load through `state`.
    def secret : String
      state.secret
    end

    def isr : Set(UInt64)
      state.isr
    end

    def apply(entry : ClusterCommand) : Nil
      current = @snapshot.get
      new_state = case entry
                  in ClusterCommand::SetSecret
                    ClusterState.new(entry.secret, current.isr)
                  in ClusterCommand::SetIsr
                    ClusterState.new(current.secret, entry.node_ids.dup)
                  in ClusterCommand
                    raise "BUG: unhandled ClusterCommand variant: #{entry.class}"
                  end
      @snapshot.set(new_state, :release)
    end

    def snapshot(io : IO) : Nil
      s = @snapshot.get
      fmt = IO::ByteFormat::LittleEndian
      io.write_bytes(SNAPSHOT_VERSION, fmt)
      io.write_bytes(s.secret.bytesize.to_u32, fmt)
      io.write(s.secret.to_slice)
      io.write_bytes(s.isr.size.to_u32, fmt)
      s.isr.each { |id| io.write_bytes(id, fmt) }
    end

    def restore(io : IO) : Nil
      fmt = IO::ByteFormat::LittleEndian
      version = io.read_bytes(UInt8, fmt)
      raise InvalidSnapshotVersion.new(version) unless version == SNAPSHOT_VERSION
      secret_len = io.read_bytes(UInt32, fmt)
      buf = Bytes.new(secret_len)
      io.read_fully(buf)
      secret = String.new(buf)
      isr_count = io.read_bytes(UInt32, fmt)
      isr = Set(UInt64).new(initial_capacity: isr_count.to_i32)
      isr_count.times { isr.add(io.read_bytes(UInt64, fmt)) }
      @snapshot.set(ClusterState.new(secret, isr), :release)
    end

    class InvalidSnapshotVersion < Exception
      def initialize(version : UInt8)
        super("Unsupported ClusterStateMachine snapshot version: #{version}")
      end
    end
  end
end

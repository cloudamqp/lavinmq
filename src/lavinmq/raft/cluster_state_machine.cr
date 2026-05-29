require "raft"
require "./cluster_command"
require "./cluster_state"

module LavinMQ::Raft
  class ClusterStateMachine < ::Raft::StateMachine(ClusterCommand)
    SNAPSHOT_VERSION = 1_u8

    # Mutex-serialized. apply mutates @secret/@isr under the lock; readers
    # acquire the lock and return references. Apply must REPLACE these fields
    # (not mutate in place) so previously-returned references stay stable.
    @mutex = Mutex.new
    @secret = ""
    @isr = Set(Int32).new

    # Consistent multi-field snapshot.
    def state : ClusterState
      @mutex.synchronize { ClusterState.new(@secret, @isr) }
    end

    def secret : String
      @mutex.synchronize { @secret }
    end

    def isr : Set(Int32)
      @mutex.synchronize { @isr }
    end

    def apply(entry : ClusterCommand) : Nil
      @mutex.synchronize do
        case entry
        in ClusterCommand::SetSecret then @secret = entry.secret
        in ClusterCommand::SetIsr    then @isr = entry.node_ids.dup
        in ClusterCommand            then raise "BUG: unhandled ClusterCommand variant: #{entry.class}"
        end
      end
    end

    def snapshot(io : IO) : Nil
      @mutex.synchronize do
        fmt = IO::ByteFormat::LittleEndian
        io.write_bytes(SNAPSHOT_VERSION, fmt)
        io.write_bytes(@secret.bytesize.to_u32, fmt)
        io.write(@secret.to_slice)
        io.write_bytes(@isr.size.to_u32, fmt)
        @isr.each { |id| io.write_bytes(id, fmt) }
      end
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
      isr = Set(Int32).new(initial_capacity: isr_count.to_i32)
      isr_count.times { isr.add(io.read_bytes(Int32, fmt)) }
      @mutex.synchronize do
        @secret = secret
        @isr = isr
      end
    end

    class InvalidSnapshotVersion < Exception
      def initialize(version : UInt8)
        super("Unsupported ClusterStateMachine snapshot version: #{version}")
      end
    end
  end
end

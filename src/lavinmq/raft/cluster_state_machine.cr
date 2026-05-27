require "raft"
require "./cluster_command"

module LavinMQ::Raft
  class ClusterStateMachine < ::Raft::StateMachine(ClusterCommand)
    SNAPSHOT_VERSION = 1_u8

    getter secret : String = ""
    getter isr : Set(UInt64) = Set(UInt64).new

    def apply(entry : ClusterCommand) : Nil
      case entry
      in ClusterCommand::SetSecret     then @secret = entry.secret
      in ClusterCommand::AddToIsr      then @isr.add(entry.node_id)
      in ClusterCommand::RemoveFromIsr then @isr.delete(entry.node_id)
        # The abstract base is unreachable (never instantiated), but Crystal
        # requires it in the exhaustive `case` when ClusterCommand is used as a
        # generic type argument (Raft::StateMachine(ClusterCommand)). Raise
        # loudly so a future concrete variant added without its own branch
        # fails here at runtime rather than being silently ignored.
      in ClusterCommand then raise "BUG: unhandled ClusterCommand variant: #{entry.class}"
      end
    end

    def snapshot(io : IO) : Nil
      fmt = IO::ByteFormat::LittleEndian
      io.write_bytes(SNAPSHOT_VERSION, fmt)
      io.write_bytes(@secret.bytesize.to_u32, fmt)
      io.write(@secret.to_slice)
      io.write_bytes(@isr.size.to_u32, fmt)
      @isr.each { |id| io.write_bytes(id, fmt) }
    end

    def restore(io : IO) : Nil
      fmt = IO::ByteFormat::LittleEndian
      version = io.read_bytes(UInt8, fmt)
      raise InvalidSnapshotVersion.new(version) unless version == SNAPSHOT_VERSION
      secret_len = io.read_bytes(UInt32, fmt)
      buf = Bytes.new(secret_len)
      io.read_fully(buf)
      @secret = String.new(buf)
      isr_count = io.read_bytes(UInt32, fmt)
      @isr.clear
      isr_count.times { @isr.add(io.read_bytes(UInt64, fmt)) }
    end

    class InvalidSnapshotVersion < Exception
      def initialize(version : UInt8)
        super("Unsupported ClusterStateMachine snapshot version: #{version}")
      end
    end
  end
end

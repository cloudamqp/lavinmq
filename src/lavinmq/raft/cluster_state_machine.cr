require "raft"
require "./cluster_command"
require "./cluster_state"

module LavinMQ::Raft
  class ClusterStateMachine < ::Raft::StateMachine(ClusterCommand)
    SNAPSHOT_VERSION = 1_u8

    # Mutex-serialized. apply mutates @isr under the lock; readers acquire the
    # lock and return the reference. Apply must REPLACE @isr (not mutate in
    # place) so previously-returned references stay stable.
    @mutex = Mutex.new
    @isr = Set(Int32).new
    # Wake-up channel for "ISR changed". apply / restore fire a non-blocking
    # `while try_send?` loop that delivers to every waiting receiver plus
    # one buffered slot for late arrivals; receivers re-check the predicate
    # they care about, so spurious wakeups are safe. Waiters use this to
    # avoid busy-polling raft state.
    getter isr_changed = ::Channel(Nil).new(1)

    # Consistent snapshot.
    def state : ClusterState
      @mutex.synchronize { ClusterState.new(@isr) }
    end

    def isr : Set(Int32)
      @mutex.synchronize { @isr }
    end

    def apply(entry : ClusterCommand) : Nil
      @mutex.synchronize do
        case entry
        in ClusterCommand::SetIsr then @isr = entry.node_ids.dup
        in ClusterCommand         then raise "BUG: unhandled ClusterCommand variant: #{entry.class}"
        end
      end
      # Signal after releasing the mutex — a woken waiter immediately reads
      # through it, and would otherwise block on the lock we still hold.
      signal_isr_changed
    end

    # Wake every consumer currently blocked on @isr_changed.receive — and,
    # for buffered(1), park one extra signal in the buffer for any late
    # arrival. `try_send?` (LavinMQ stdlib extension) is non-blocking and
    # returns false once nothing more can accept the signal.
    private def signal_isr_changed : Nil
      while @isr_changed.try_send?(nil)
      end
    end

    def snapshot(io : IO) : Nil
      @mutex.synchronize do
        fmt = IO::ByteFormat::LittleEndian
        io.write_bytes(SNAPSHOT_VERSION, fmt)
        io.write_bytes(@isr.size.to_u32, fmt)
        @isr.each { |id| io.write_bytes(id, fmt) }
      end
    end

    def restore(io : IO) : Nil
      fmt = IO::ByteFormat::LittleEndian
      version = io.read_bytes(UInt8, fmt)
      raise InvalidSnapshotVersion.new(version) unless version == SNAPSHOT_VERSION
      isr_count = io.read_bytes(UInt32, fmt)
      isr = Set(Int32).new(initial_capacity: isr_count.to_i32)
      isr_count.times { isr.add(io.read_bytes(Int32, fmt)) }
      @mutex.synchronize do
        @isr = isr
      end
      signal_isr_changed
    end

    class InvalidSnapshotVersion < Exception
      def initialize(version : UInt8)
        super("Unsupported ClusterStateMachine snapshot version: #{version}")
      end
    end
  end
end

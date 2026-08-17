require "../clustering"
require "wait_group"

module LavinMQ
  module Clustering
    # An instruction for the follower, carried out by the leader's control_loop
    # rather than by the fiber that asks for it (see Follower#control).
    #
    # Whoever takes a packet must #done it exactly once, whether or not it got
    # as far as #to_io: one dropped without a #done hangs its waiter forever.
    abstract struct ControlPacket
      # Records whose path starts with this carry an instruction, not file data.
      # Never a real path, so nothing under it is created on disk or tracked.
      PREFIX = "$ctrl/"

      # True for a record that carries an instruction rather than file data,
      # whether or not this build knows the instruction: an unknown one must
      # still be recognised as a control record and skipped, not read as a
      # delete of its path. Which instruction it is, .from_str answers.
      def self.control?(path : String) : Bool
        path.starts_with?(PREFIX)
      end

      # The packet a received record's path stands for, nil for an instruction
      # only a newer leader knows. Deliberately returns the narrow union of the
      # packets with a wire form, not ControlPacket?, so a `case ... in` on it
      # stops compiling when a new one is added (see Client#control).
      def self.from_str(str : String)
        case str
        when SyncControlPacket::PATH then SyncControlPacket.new
        end
      end

      # How many bytes #to_io writes. Counted into the follower's sent-byte
      # total at the position the record occupies on the wire.
      abstract def bytesize : Int64

      # Act on the follower's stream. Runs under the follower's @write_lock:
      # touch the given IO and nothing else — no blocking, no other locks.
      abstract def to_io(io : IO) : Nil

      # Queued to one more follower; counted per follower, not per request.
      def add : Nil
      end

      # Handled by one more follower: written, or given up on as dead.
      def done : Nil
      end
    end

    # Pushes what's buffered to the follower. Writes nothing itself, but
    # everything queued ahead of it has been written by the time it runs — so a
    # released `wg` means those bytes are on the socket, not in the compressor.
    struct FlushPacket < ControlPacket
      def initialize(@wg : WaitGroup? = nil)
      end

      def bytesize : Int64
        0i64 # a flush adds nothing to the stream
      end

      def add : Nil
        @wg.try &.add
      end

      def done : Nil
        @wg.try &.done
      end

      def to_io(io : IO) : Nil
        io.flush
      end
    end

    # Asks the follower to make everything replicated so far durable. It fsyncs
    # before acking this record (see Client#control), so this ack — unlike an
    # ordinary one, which only means received and applied — means persisted.
    #
    # No waiter of its own: the queue is FIFO, so waiting for a FlushPacket
    # queued after it covers this record too. Must always be followed by one, or
    # it sits in the compressor until ack_loop's fallback flush.
    struct SyncControlPacket < ControlPacket
      # Names this instruction on the wire; the empty body means the record is
      # routed by prefix before its length is interpreted.
      PATH = "#{PREFIX}sync"

      # The whole record: filename framing plus a zero length, no body.
      RECORD = begin
        io = IO::Memory.new
        io.write_bytes PATH.bytesize.to_i32, IO::ByteFormat::LittleEndian
        io.write PATH.to_slice
        io.write_bytes 0i64 # empty body (endian-agnostic)
        io.to_slice
      end

      def bytesize : Int64
        RECORD.bytesize.to_i64
      end

      def to_io(io : IO) : Nil
        io.write RECORD
      end
    end
  end
end

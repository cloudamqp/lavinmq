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
      # The symbol of every packet that can be sent. A record whose filename
      # starts with one carries that instruction instead of file data; what
      # follows the symbol is the instruction's argument.
      SYMBOLS = {SyncControlPacket::SYMBOL}

      # True for a record that carries an instruction rather than file data.
      # Which instruction it is, .from_str answers.
      def self.control?(path : String) : Bool
        SYMBOLS.includes? path[0]?
      end

      # The packet a received record's filename stands for, nil for a symbol
      # this build doesn't know. Deliberately returns the narrow union of the
      # packets with a wire form, not ControlPacket?, so a `case ... in` on it
      # stops compiling when a new one is added (see Client#control).
      def self.from_str(str : String)
        case str[0]?
        when SyncControlPacket::SYMBOL then SyncControlPacket.new(str[1..])
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

    # Asks the follower to make replicated writes durable: the file `path`
    # names, or the whole filesystem when it names a directory — the empty path
    # being the data dir itself. The follower syncs before acking this record
    # (see Client#control), so this ack — unlike an ordinary one, which only
    # means received and applied — means persisted.
    #
    # No waiter of its own: the queue is FIFO, so waiting for a FlushPacket
    # queued after it covers this record too. Must always be followed by one, or
    # it sits in the compressor until ack_loop's fallback flush.
    struct SyncControlPacket < ControlPacket
      # Names this instruction on the wire; `path` follows it as the argument.
      SYMBOL = '$'

      getter path : String

      def initialize(@path : String = "")
      end

      def bytesize : Int64
        (sizeof(Int32) + SYMBOL.bytesize + @path.bytesize + sizeof(Int64)).to_i64
      end

      # Written symbol-then-path rather than from a joined string, so sending
      # one allocates nothing.
      def to_io(io : IO) : Nil
        io.write_bytes (SYMBOL.bytesize + @path.bytesize).to_i32, IO::ByteFormat::LittleEndian
        io << SYMBOL << @path
        io.write_bytes 0i64 # empty body (endian-agnostic)
      end
    end
  end
end

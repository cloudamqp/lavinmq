module LavinMQ::Clustering::Raft
  # A log entry. The replicated state machine is just the ISR, and every
  # entry carries the full set (nil for the no-op a new leader appends), so
  # the latest entry is the whole state and compaction is trivial.
  record Entry, term : Int64, isr : Set(Int32)?

  # `from` is the sender's raft address. Messages are one-way: replies are
  # sent back as their own message over the sender's outbound connection.
  record RequestVote, from : String, term : Int64, node_id : Int32,
    last_log_index : Int64, last_log_term : Int64, pre_vote : Bool, transfer : Bool

  record VoteResponse, from : String, term : Int64, granted : Bool, pre_vote : Bool

  record AppendEntries, from : String, term : Int64, leader_uri : String,
    prev_index : Int64, prev_term : Int64, entries : Array(Entry), commit : Int64

  # On failure match_index is a hint of the follower's last index.
  record AppendResponse, from : String, term : Int64, node_id : Int32, success : Bool, match_index : Int64

  record InstallSnapshot, from : String, term : Int64, leader_uri : String,
    index : Int64, snapshot_term : Int64, isr : Set(Int32)?

  # Sent by a leader shutting down gracefully, so an up-to-date follower
  # campaigns at once instead of waiting out its election timeout.
  record TimeoutNow, from : String, term : Int64

  alias Message = RequestVote | VoteResponse | AppendEntries | AppendResponse | InstallSnapshot | TimeoutNow

  module Codec
    extend self

    Format = IO::ByteFormat::LittleEndian

    # Frames above this are rejected, a peer sending one is broken or hostile.
    MAX_FRAME = 1 << 20

    def encode(msg : Message) : Bytes
      io = IO::Memory.new
      case msg
      in RequestVote
        io.write_byte 1u8
        write_str io, msg.from
        io.write_bytes msg.term, Format
        io.write_bytes msg.node_id, Format
        io.write_bytes msg.last_log_index, Format
        io.write_bytes msg.last_log_term, Format
        io.write_byte msg.pre_vote ? 1u8 : 0u8
        io.write_byte msg.transfer ? 1u8 : 0u8
      in VoteResponse
        io.write_byte 2u8
        write_str io, msg.from
        io.write_bytes msg.term, Format
        io.write_byte msg.granted ? 1u8 : 0u8
        io.write_byte msg.pre_vote ? 1u8 : 0u8
      in AppendEntries
        io.write_byte 3u8
        write_str io, msg.from
        io.write_bytes msg.term, Format
        write_str io, msg.leader_uri
        io.write_bytes msg.prev_index, Format
        io.write_bytes msg.prev_term, Format
        io.write_bytes msg.commit, Format
        io.write_bytes msg.entries.size, Format
        msg.entries.each do |e|
          io.write_bytes e.term, Format
          write_isr io, e.isr
        end
      in AppendResponse
        io.write_byte 4u8
        write_str io, msg.from
        io.write_bytes msg.term, Format
        io.write_bytes msg.node_id, Format
        io.write_byte msg.success ? 1u8 : 0u8
        io.write_bytes msg.match_index, Format
      in InstallSnapshot
        io.write_byte 5u8
        write_str io, msg.from
        io.write_bytes msg.term, Format
        write_str io, msg.leader_uri
        io.write_bytes msg.index, Format
        io.write_bytes msg.snapshot_term, Format
        write_isr io, msg.isr
      in TimeoutNow
        io.write_byte 6u8
        write_str io, msg.from
        io.write_bytes msg.term, Format
      end
      io.to_slice
    end

    def decode(bytes : Bytes) : Message
      io = IO::Memory.new(bytes, writable: false)
      type = io.read_byte || raise IO::EOFError.new
      from = read_str(io)
      term = io.read_bytes Int64, Format
      case type
      when 1
        RequestVote.new(from, term, io.read_bytes(Int32, Format), io.read_bytes(Int64, Format),
          io.read_bytes(Int64, Format), read_bool(io), read_bool(io))
      when 2
        VoteResponse.new(from, term, read_bool(io), read_bool(io))
      when 3
        leader_uri = read_str(io)
        prev_index = io.read_bytes Int64, Format
        prev_term = io.read_bytes Int64, Format
        commit = io.read_bytes Int64, Format
        count = io.read_bytes Int32, Format
        raise IO::Error.new("Invalid entry count #{count}") unless 0 <= count <= bytes.size
        entries = Array(Entry).new(count) do
          Entry.new(io.read_bytes(Int64, Format), read_isr(io))
        end
        AppendEntries.new(from, term, leader_uri, prev_index, prev_term, entries, commit)
      when 4
        AppendResponse.new(from, term, io.read_bytes(Int32, Format), read_bool(io), io.read_bytes(Int64, Format))
      when 5
        leader_uri = read_str(io)
        InstallSnapshot.new(from, term, leader_uri, io.read_bytes(Int64, Format),
          io.read_bytes(Int64, Format), read_isr(io))
      when 6
        TimeoutNow.new(from, term)
      else
        raise IO::Error.new("Unknown raft message type #{type}")
      end
    end

    def write_isr(io, isr : Set(Int32)?) : Nil
      if isr
        io.write_bytes isr.size, Format
        isr.each { |id| io.write_bytes id, Format }
      else
        io.write_bytes -1, Format
      end
    end

    def read_isr(io) : Set(Int32)?
      size = io.read_bytes Int32, Format
      return if size < 0
      raise IO::Error.new("Invalid ISR size #{size}") if size > MAX_FRAME // 4
      set = Set(Int32).new(size)
      size.times { set << io.read_bytes(Int32, Format) }
      set
    end

    private def write_str(io, str : String) : Nil
      io.write_bytes str.bytesize, Format
      io.write str.to_slice
    end

    private def read_str(io) : String
      len = io.read_bytes Int32, Format
      raise IO::Error.new("Invalid string length #{len}") unless 0 <= len <= MAX_FRAME
      io.read_string(len)
    end

    private def read_bool(io) : Bool
      (io.read_byte || raise IO::EOFError.new) != 0
    end
  end
end

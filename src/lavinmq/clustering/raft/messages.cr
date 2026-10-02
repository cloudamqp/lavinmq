module LavinMQ::Clustering::Raft
  # The cluster configuration, as raft addresses. Voters count towards
  # quorum, commit and elections. Learners only receive the log, they are
  # added first and promoted once they've caught up.
  record Membership, voters : Set(String), learners : Set(String) do
    def members : Set(String)
      voters | learners
    end

    def includes?(addr : String) : Bool
      voters.includes?(addr) || learners.includes?(addr)
    end
  end

  # A log entry. The replicated state machine is the ISR and the membership,
  # and every entry carries the full value of whichever it changes (nil for
  # the other, and for both in the no-op a new leader appends, unless it's the
  # first leader), so the latest non-nil value is the whole state and
  # compaction is trivial.
  record Entry, term : Int64, isr : Set(Int32)?, membership : Membership? = nil

  # `from` is the sender's raft address. Messages are one-way: replies are
  # sent back as their own message over the sender's outbound connection.
  record RequestVote, from : String, term : Int64, node_id : Int32,
    last_log_index : Int64, last_log_term : Int64, pre_vote : Bool, transfer : Bool

  record VoteResponse, from : String, term : Int64, granted : Bool, pre_vote : Bool

  record AppendEntries, from : String, term : Int64, node_id : Int32, leader_uri : String,
    prev_index : Int64, prev_term : Int64, entries : Array(Entry), commit : Int64

  # On failure match_index is a hint of the follower's last index.
  record AppendResponse, from : String, term : Int64, node_id : Int32, success : Bool, match_index : Int64

  record InstallSnapshot, from : String, term : Int64, node_id : Int32, leader_uri : String,
    index : Int64, snapshot_term : Int64, isr : Set(Int32)?, membership : Membership? = nil

  # Sent by a leader shutting down gracefully, so an up-to-date follower
  # campaigns at once instead of waiting out its election timeout.
  record TimeoutNow, from : String, term : Int64

  # A voter's whole log, sent to an in-ISR candidate whose log is older, see
  # Core#handle_catch_up.
  record CatchUp, from : String, term : Int64, node_id : Int32,
    snapshot_index : Int64, snapshot_term : Int64, snapshot_isr : Set(Int32)?, entries : Array(Entry),
    snapshot_membership : Membership? = nil

  alias Message = RequestVote | VoteResponse | AppendEntries | AppendResponse | InstallSnapshot | TimeoutNow | CatchUp

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
        io.write_bytes msg.node_id, Format
        write_str io, msg.leader_uri
        io.write_bytes msg.prev_index, Format
        io.write_bytes msg.prev_term, Format
        io.write_bytes msg.commit, Format
        write_entries io, msg.entries
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
        io.write_bytes msg.node_id, Format
        write_str io, msg.leader_uri
        io.write_bytes msg.index, Format
        io.write_bytes msg.snapshot_term, Format
        write_isr io, msg.isr
        write_membership io, msg.membership
      in TimeoutNow
        io.write_byte 6u8
        write_str io, msg.from
        io.write_bytes msg.term, Format
      in CatchUp
        io.write_byte 7u8
        write_str io, msg.from
        io.write_bytes msg.term, Format
        io.write_bytes msg.node_id, Format
        io.write_bytes msg.snapshot_index, Format
        io.write_bytes msg.snapshot_term, Format
        write_isr io, msg.snapshot_isr
        write_entries io, msg.entries
        write_membership io, msg.snapshot_membership
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
        node_id = io.read_bytes Int32, Format
        leader_uri = read_str(io)
        prev_index = io.read_bytes Int64, Format
        prev_term = io.read_bytes Int64, Format
        commit = io.read_bytes Int64, Format
        AppendEntries.new(from, term, node_id, leader_uri, prev_index, prev_term, read_entries(io, bytes.size), commit)
      when 4
        AppendResponse.new(from, term, io.read_bytes(Int32, Format), read_bool(io), io.read_bytes(Int64, Format))
      when 5
        node_id = io.read_bytes Int32, Format
        leader_uri = read_str(io)
        InstallSnapshot.new(from, term, node_id, leader_uri, io.read_bytes(Int64, Format),
          io.read_bytes(Int64, Format), read_isr(io), read_membership(io))
      when 6
        TimeoutNow.new(from, term)
      when 7
        CatchUp.new(from, term, io.read_bytes(Int32, Format), io.read_bytes(Int64, Format),
          io.read_bytes(Int64, Format), read_isr(io), read_entries(io, bytes.size), read_membership(io))
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

    def write_membership(io, membership : Membership?) : Nil
      unless membership
        io.write_byte 0u8
        return
      end
      io.write_byte 1u8
      write_addrs io, membership.voters
      write_addrs io, membership.learners
    end

    def read_membership(io) : Membership?
      return unless read_bool(io)
      Membership.new(read_addrs(io), read_addrs(io))
    end

    def write_entries(io, entries : Array(Entry)) : Nil
      io.write_bytes entries.size, Format
      entries.each do |e|
        io.write_bytes e.term, Format
        write_isr io, e.isr
        write_membership io, e.membership
      end
    end

    # `max` bounds the count by what the input could possibly hold.
    def read_entries(io, max : Int32, version = 3) : Array(Entry)
      count = io.read_bytes Int32, Format
      raise IO::Error.new("Invalid entry count #{count}") unless 0 <= count <= max
      Array(Entry).new(count) do
        Entry.new(io.read_bytes(Int64, Format), read_isr(io), version >= 3 ? read_membership(io) : nil)
      end
    end

    private def write_addrs(io, addrs : Set(String)) : Nil
      io.write_bytes addrs.size, Format
      addrs.each { |a| write_str io, a }
    end

    private def read_addrs(io) : Set(String)
      size = io.read_bytes Int32, Format
      raise IO::Error.new("Invalid member count #{size}") unless 0 <= size <= MAX_FRAME // 4
      set = Set(String).new(size)
      size.times { set << read_str(io) }
      set
    end

    def write_str(io, str : String) : Nil
      io.write_bytes str.bytesize, Format
      io.write str.to_slice
    end

    def read_str(io) : String
      len = io.read_bytes Int32, Format
      raise IO::Error.new("Invalid string length #{len}") unless 0 <= len <= MAX_FRAME
      io.read_string(len)
    end

    def read_bool(io) : Bool
      (io.read_byte || raise IO::EOFError.new) != 0
    end
  end
end

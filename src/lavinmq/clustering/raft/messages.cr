module LavinMQ::Clustering::Raft
  # The cluster configuration. Nodes are known by their clustering id, the
  # address is where to reach them, so a node that moves keeps its identity.
  # Voters count towards quorum, commit and elections. Learners only receive
  # the log, they are added first and promoted once they've caught up.
  # `relocated` are learners that were voters until they showed up at a new
  # address, the leader promotes them back once they're eligible.
  record Membership, voters : Set(Int32), learners : Set(Int32), addresses : Hash(Int32, String),
    relocated = Set(Int32).new do
    def members : Set(Int32)
      voters | learners
    end

    def includes?(id : Int32) : Bool
      voters.includes?(id) || learners.includes?(id)
    end
  end

  # A log entry. The replicated state machine is the ISR and the membership,
  # and every entry carries the full value of whichever it changes (nil for
  # the other, and for both in the no-op a new leader appends, unless it's the
  # first leader), so the latest non-nil value is the whole state and
  # compaction is trivial.
  record Entry, term : Int64, isr : Set(Int32)?, membership : Membership? = nil

  # `from` is the sender's clustering id. Messages are one-way: replies are
  # sent back as their own message over the sender's outbound connection.
  record RequestVote, from : Int32, term : Int64,
    last_log_index : Int64, last_log_term : Int64, pre_vote : Bool, transfer : Bool

  record VoteResponse, from : Int32, term : Int64, granted : Bool, pre_vote : Bool

  record AppendEntries, from : Int32, term : Int64, leader_uri : String,
    prev_index : Int64, prev_term : Int64, entries : Array(Entry), commit : Int64

  # On failure match_index is a hint of the follower's last index.
  record AppendResponse, from : Int32, term : Int64, success : Bool, match_index : Int64

  record InstallSnapshot, from : Int32, term : Int64, leader_uri : String,
    index : Int64, snapshot_term : Int64, isr : Set(Int32)?, membership : Membership? = nil

  # Sent by a leader shutting down gracefully, so an up-to-date follower
  # campaigns at once instead of waiting out its election timeout.
  record TimeoutNow, from : Int32, term : Int64

  # A voter's whole log, sent to an in-ISR candidate whose log is older, see
  # Core#handle_catch_up.
  record CatchUp, from : Int32, term : Int64,
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
      encode(msg, io)
      io.to_slice
    end

    def encode(msg : Message, io : IO) : Nil
      case msg
      in RequestVote
        io.write_byte 1u8
        io.write_bytes msg.from, Format
        io.write_bytes msg.term, Format
        io.write_bytes msg.last_log_index, Format
        io.write_bytes msg.last_log_term, Format
        io.write_byte msg.pre_vote ? 1u8 : 0u8
        io.write_byte msg.transfer ? 1u8 : 0u8
      in VoteResponse
        io.write_byte 2u8
        io.write_bytes msg.from, Format
        io.write_bytes msg.term, Format
        io.write_byte msg.granted ? 1u8 : 0u8
        io.write_byte msg.pre_vote ? 1u8 : 0u8
      in AppendEntries
        io.write_byte 3u8
        io.write_bytes msg.from, Format
        io.write_bytes msg.term, Format
        write_str io, msg.leader_uri
        io.write_bytes msg.prev_index, Format
        io.write_bytes msg.prev_term, Format
        io.write_bytes msg.commit, Format
        write_entries io, msg.entries
      in AppendResponse
        io.write_byte 4u8
        io.write_bytes msg.from, Format
        io.write_bytes msg.term, Format
        io.write_byte msg.success ? 1u8 : 0u8
        io.write_bytes msg.match_index, Format
      in InstallSnapshot
        io.write_byte 5u8
        io.write_bytes msg.from, Format
        io.write_bytes msg.term, Format
        write_str io, msg.leader_uri
        io.write_bytes msg.index, Format
        io.write_bytes msg.snapshot_term, Format
        write_isr io, msg.isr
        write_membership io, msg.membership
      in TimeoutNow
        io.write_byte 6u8
        io.write_bytes msg.from, Format
        io.write_bytes msg.term, Format
      in CatchUp
        io.write_byte 7u8
        io.write_bytes msg.from, Format
        io.write_bytes msg.term, Format
        io.write_bytes msg.snapshot_index, Format
        io.write_bytes msg.snapshot_term, Format
        write_isr io, msg.snapshot_isr
        write_entries io, msg.entries
        write_membership io, msg.snapshot_membership
      end
    end

    # Copies what it needs from `bytes`, which can be reused afterwards.
    def decode(bytes : Bytes) : Message
      io = IO::Memory.new(bytes, writable: false)
      type = io.read_byte || raise IO::EOFError.new
      from = io.read_bytes Int32, Format
      term = io.read_bytes Int64, Format
      case type
      when 1
        RequestVote.new(from, term, io.read_bytes(Int64, Format), io.read_bytes(Int64, Format),
          read_bool(io), read_bool(io))
      when 2
        VoteResponse.new(from, term, read_bool(io), read_bool(io))
      when 3
        leader_uri = read_str(io)
        prev_index = io.read_bytes Int64, Format
        prev_term = io.read_bytes Int64, Format
        commit = io.read_bytes Int64, Format
        AppendEntries.new(from, term, leader_uri, prev_index, prev_term, read_entries(io, bytes.size), commit)
      when 4
        AppendResponse.new(from, term, read_bool(io), io.read_bytes(Int64, Format))
      when 5
        leader_uri = read_str(io)
        InstallSnapshot.new(from, term, leader_uri, io.read_bytes(Int64, Format),
          io.read_bytes(Int64, Format), read_isr(io), read_membership(io))
      when 6
        TimeoutNow.new(from, term)
      when 7
        CatchUp.new(from, term, io.read_bytes(Int64, Format),
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
      write_isr io, membership.voters
      write_isr io, membership.learners
      write_isr io, membership.relocated
      io.write_bytes membership.addresses.size, Format
      membership.addresses.each do |id, addr|
        io.write_bytes id, Format
        write_str io, addr
      end
    end

    def read_membership(io) : Membership?
      return unless read_bool(io)
      voters = read_ids(io)
      learners = read_ids(io)
      relocated = read_ids(io)
      size = io.read_bytes Int32, Format
      raise IO::Error.new("Invalid member count #{size}") unless 0 <= size <= MAX_FRAME // 8
      addresses = Hash(Int32, String).new(initial_capacity: size)
      size.times { addresses[io.read_bytes(Int32, Format)] = read_str(io) }
      Membership.new(voters, learners, addresses, relocated)
    end

    private def read_ids(io) : Set(Int32)
      read_isr(io) || raise IO::Error.new("Missing member ids")
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
    def read_entries(io, max : Int32) : Array(Entry)
      count = io.read_bytes Int32, Format
      raise IO::Error.new("Invalid entry count #{count}") unless 0 <= count <= max
      Array(Entry).new(count) do
        Entry.new(io.read_bytes(Int64, Format), read_isr(io), read_membership(io))
      end
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

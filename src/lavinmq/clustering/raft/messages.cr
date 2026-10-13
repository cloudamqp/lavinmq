require "json"

module LavinMQ::Clustering::Raft
  # The cluster configuration. Nodes are known by their clustering id, the
  # address is where to reach them, so a node that moves keeps its identity.
  # Voters count towards quorum, commit and elections. Learners only receive
  # the log, they are added first and promoted once they've caught up.
  # `relocated` are learners that were voters until they showed up at a new
  # address, the leader promotes them back once they're eligible.
  record Membership, voters : Set(Int32), learners : Set(Int32), addresses : Hash(Int32, String),
    relocated = Set(Int32).new do
    include JSON::Serializable

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
  record Entry, term : Int64, isr : Set(Int32)?, membership : Membership? = nil do
    include JSON::Serializable
  end

  # `from` is the sender's clustering id. Messages are one-way: replies are
  # sent back as their own message over the sender's outbound connection.
  record RequestVote, from : Int32, term : Int64,
    last_log_index : Int64, last_log_term : Int64, pre_vote : Bool, transfer : Bool do
    include JSON::Serializable
  end

  record VoteResponse, from : Int32, term : Int64, granted : Bool, pre_vote : Bool do
    include JSON::Serializable
  end

  record AppendEntries, from : Int32, term : Int64, leader_uri : String,
    prev_index : Int64, prev_term : Int64, entries : Array(Entry), commit : Int64 do
    include JSON::Serializable
  end

  # On failure match_index is a hint of the follower's last index.
  record AppendResponse, from : Int32, term : Int64, success : Bool, match_index : Int64 do
    include JSON::Serializable
  end

  record InstallSnapshot, from : Int32, term : Int64, leader_uri : String,
    index : Int64, snapshot_term : Int64, isr : Set(Int32)?, membership : Membership? = nil do
    include JSON::Serializable
  end

  # Sent by a leader shutting down gracefully, so an up-to-date follower
  # campaigns at once instead of waiting out its election timeout.
  record TimeoutNow, from : Int32, term : Int64 do
    include JSON::Serializable
  end

  # A voter's whole log, sent to an in-ISR candidate whose log is older, see
  # Core#handle_catch_up.
  record CatchUp, from : Int32, term : Int64,
    snapshot_index : Int64, snapshot_term : Int64, snapshot_isr : Set(Int32)?, entries : Array(Entry),
    snapshot_membership : Membership? = nil do
    include JSON::Serializable
  end

  alias Message = RequestVote | VoteResponse | AppendEntries | AppendResponse | InstallSnapshot | TimeoutNow | CatchUp

  # Frames a message as a type byte and its JSON. Raft traffic is a few
  # small messages a second, so a hand-written binary format buys nothing.
  module Codec
    extend self

    Format = IO::ByteFormat::LittleEndian

    # Frames above this are rejected, a peer sending one is broken or hostile.
    MAX_FRAME = 1 << 20

    TYPES = {RequestVote, VoteResponse, AppendEntries, AppendResponse, InstallSnapshot, TimeoutNow, CatchUp}

    def encode(msg : Message) : Bytes
      io = IO::Memory.new
      encode(msg, io)
      io.to_slice
    end

    def encode(msg : Message, io : IO) : Nil
      {% begin %}
        case msg
        {% for type, i in TYPES %}
        in {{ type }} then io.write_byte {{ i + 1 }}u8
        {% end %}
        end
      {% end %}
      msg.to_json(io)
    end

    def decode(bytes : Bytes) : Message
      decode(IO::Memory.new(bytes, writable: false), bytes.size)
    end

    # A message of *size* bytes from *io*
    def decode(io : IO, size : Int32) : Message
      type = io.read_byte || raise IO::EOFError.new
      json = io.read_string(size - 1)
      {% begin %}
        case type
        {% for type, i in TYPES %}
        when {{ i + 1 }} then {{ type }}.from_json(json)
        {% end %}
        else raise IO::Error.new("Unknown raft message type #{type}")
        end
      {% end %}
    rescue ex : JSON::ParseException | JSON::SerializableError
      raise IO::Error.new("Invalid raft message: #{ex.message}")
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
  end
end

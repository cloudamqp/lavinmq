require "openssl/hmac"
require "random/secure"
require "../mfile"

module LavinMQ
  module ClusteringV2
    MAGIC            = "LMQ2".to_slice
    PROTOCOL_VERSION = 1_u8
    CHALLENGE_SIZE   =   32
    DIGEST_SIZE      =   32

    enum Role : UInt8
      Follower     = 0
      PreCandidate = 1
      Candidate    = 2
      Leader       = 3
      Learner      = 4
    end

    enum MemberRole : UInt8
      Voter   = 0
      Learner = 1
    end

    enum ConfChangeType : UInt8
      AddLearner     = 0
      PromoteLearner = 1
      RemoveNode     = 2
    end

    enum LogCommandType : UInt8
      LeaderNoOp         =  0
      ConfChange         =  1
      Publish            =  2
      Ack                =  3
      SegmentRoll        =  4
      SegmentDelete      =  5
      StreamOffsetCommit =  6
      MqttRetainPut      =  7
      MqttRetainDelete   =  8
      VHostCreate        =  9
      VHostDelete        = 10
      VHostLimits        = 11
      UserPut            = 12
      UserDelete         = 13
      QueueDeclare       = 14
      QueueDelete        = 15
      QueuePurge         = 16
      ExchangeDeclare    = 17
      ExchangeDelete     = 18
      BindingAdd         = 19
      BindingRemove      = 20
      PolicyPut          = 21
      PolicyDelete       = 22
      ParameterPut       = 23
      ParameterDelete    = 24
      Tombstone          = 25
      FileReplace        = 26
    end

    enum FrameType : UInt8
      HandshakeHello          =  1
      HandshakeChallenge      =  2
      HandshakeResponse       =  3
      HandshakeAck            =  4
      RequestPreVote          =  5
      RequestPreVoteResponse  =  6
      RequestVote             =  7
      RequestVoteResponse     =  8
      AppendEntries           =  9
      AppendEntriesResponse   = 10
      InstallSnapshot         = 11
      InstallSnapshotResponse = 12
    end

    # Fixed 48-byte index entry stored in the mmap'd `raft.idx` ring buffer.
    # Zero heap allocations on read/write.
    @[Packed]
    struct LogIndexEntry
      property term : UInt64
      property lsn : UInt64
      property vhost_id : UInt64
      property queue_id : UInt64
      property segment_id : UInt32
      property position : UInt32
      property byte_size : UInt32
      property cmd_type : UInt8
      property flags : UInt8
      property pad : UInt16

      FLAG_TOMBSTONED  = 0x01_u8
      FLAG_HAS_CTRL    = 0x02_u8
      FLAG_FROM_LEADER = 0x04_u8

      def initialize(@term : UInt64 = 0_u64,
                     @lsn : UInt64 = 0_u64,
                     @vhost_id : UInt64 = 0_u64,
                     @cmd_type : UInt8 = 0_u8,
                     @flags : UInt8 = 0_u8,
                     @queue_id : UInt64 = 0_u64,
                     @segment_id : UInt32 = 0_u32,
                     @position : UInt32 = 0_u32,
                     @byte_size : UInt32 = 0_u32,
                     @pad : UInt16 = 0_u16)
      end

      def command : LogCommandType
        LogCommandType.from_value(@cmd_type)
      end

      def tombstoned? : Bool
        (@flags & FLAG_TOMBSTONED) != 0
      end

      def has_ctrl_payload? : Bool
        (@flags & FLAG_HAS_CTRL) != 0
      end

      def from_leader? : Bool
        (@flags & FLAG_FROM_LEADER) != 0
      end
    end

    struct MemberInfo
      getter node_id : UInt64
      getter peer_uri : String
      getter amqp_uri : String
      getter http_uri : String
      property role : MemberRole

      def initialize(@node_id : UInt64,
                     @peer_uri : String,
                     @amqp_uri : String = "",
                     @http_uri : String = "",
                     @role : MemberRole = MemberRole::Voter)
      end

      def voter? : Bool
        @role.voter?
      end

      def learner? : Bool
        @role.learner?
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : Nil
        format.encode(@node_id, io)
        io.write_byte(@role.value)
        Protocol.write_string(io, @peer_uri, format)
        Protocol.write_string(io, @amqp_uri, format)
        Protocol.write_string(io, @http_uri, format)
      end

      def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
        node_id = format.decode(UInt64, io)
        role_byte = io.read_byte || raise IO::EOFError.new
        role = MemberRole.from_value(role_byte)
        peer_uri = Protocol.read_string(io, format)
        amqp_uri = Protocol.read_string(io, format)
        http_uri = Protocol.read_string(io, format)
        new(node_id, peer_uri, amqp_uri, http_uri, role)
      end
    end

    struct ConfChangeCommand
      getter change_type : ConfChangeType
      getter member : MemberInfo

      def initialize(@change_type : ConfChangeType, @member : MemberInfo)
      end

      def to_slice : Bytes
        io = IO::Memory.new(64)
        to_io(io)
        io.to_slice
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : Nil
        io.write_byte(@change_type.value)
        @member.to_io(io, format)
      end

      def self.from_slice(bytes : Bytes) : self
        from_io(IO::Memory.new(bytes))
      end

      def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
        type_byte = io.read_byte || raise IO::EOFError.new
        change_type = ConfChangeType.from_value(type_byte)
        member = MemberInfo.from_io(io, format)
        new(change_type, member)
      end
    end

    struct VoteRequest
      getter? pre_vote : Bool
      getter term : UInt64
      getter candidate_id : UInt64
      getter last_log_lsn : UInt64
      getter last_log_term : UInt64

      def initialize(@pre_vote : Bool,
                     @term : UInt64,
                     @candidate_id : UInt64,
                     @last_log_lsn : UInt64,
                     @last_log_term : UInt64)
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : Nil
        io.write_byte(@pre_vote ? 1_u8 : 0_u8)
        format.encode(@term, io)
        format.encode(@candidate_id, io)
        format.encode(@last_log_lsn, io)
        format.encode(@last_log_term, io)
      end

      def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
        pre_vote = (io.read_byte || raise IO::EOFError.new) != 0_u8
        term = format.decode(UInt64, io)
        candidate_id = format.decode(UInt64, io)
        last_log_lsn = format.decode(UInt64, io)
        last_log_term = format.decode(UInt64, io)
        new(pre_vote, term, candidate_id, last_log_lsn, last_log_term)
      end
    end

    struct VoteResponse
      getter? pre_vote : Bool
      getter term : UInt64
      getter voter_id : UInt64
      getter? vote_granted : Bool

      def initialize(@pre_vote : Bool,
                     @term : UInt64,
                     @voter_id : UInt64,
                     @vote_granted : Bool)
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : Nil
        io.write_byte(@pre_vote ? 1_u8 : 0_u8)
        format.encode(@term, io)
        format.encode(@voter_id, io)
        io.write_byte(@vote_granted ? 1_u8 : 0_u8)
      end

      def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
        pre_vote = (io.read_byte || raise IO::EOFError.new) != 0_u8
        term = format.decode(UInt64, io)
        voter_id = format.decode(UInt64, io)
        granted = (io.read_byte || raise IO::EOFError.new) != 0_u8
        new(pre_vote, term, voter_id, granted)
      end
    end

    # Wire representation of a single replicated log entry.
    struct LogEntryRecord
      getter entry : LogIndexEntry
      getter payload : Bytes

      def initialize(@entry : LogIndexEntry, @payload : Bytes = Bytes.empty)
      end

      def term : UInt64
        @entry.term
      end

      def lsn : UInt64
        @entry.lsn
      end

      def command : LogCommandType
        @entry.command
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : Nil
        format.encode(@entry.term, io)
        format.encode(@entry.lsn, io)
        format.encode(@entry.vhost_id, io)
        io.write_byte(@entry.cmd_type)
        io.write_byte(@entry.flags)
        format.encode(@entry.queue_id, io)
        format.encode(@entry.segment_id, io)
        format.encode(@entry.position, io)
        format.encode(@payload.size.to_u32, io)
        io.write(@payload) unless @payload.empty?
      end

      def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
        term = format.decode(UInt64, io)
        lsn = format.decode(UInt64, io)
        vhost_id = format.decode(UInt64, io)
        cmd_type = io.read_byte || raise IO::EOFError.new
        flags = io.read_byte || raise IO::EOFError.new
        queue_id = format.decode(UInt64, io)
        segment_id = format.decode(UInt32, io)
        position = format.decode(UInt32, io)
        payload_len = format.decode(UInt32, io)
        payload = if payload_len > 0
                    buf = Bytes.new(payload_len)
                    io.read_fully(buf)
                    buf
                  else
                    Bytes.empty
                  end
        idx = LogIndexEntry.new(term, lsn, vhost_id, cmd_type, flags, queue_id, segment_id, position, payload_len)
        new(idx, payload)
      end
    end

    struct AppendEntriesRequest
      getter term : UInt64
      getter leader_id : UInt64
      getter prev_log_lsn : UInt64
      getter prev_log_term : UInt64
      getter leader_commit : UInt64
      getter entries : Array(LogEntryRecord)

      def initialize(@term : UInt64,
                     @leader_id : UInt64,
                     @prev_log_lsn : UInt64,
                     @prev_log_term : UInt64,
                     @leader_commit : UInt64,
                     @entries : Array(LogEntryRecord) = Array(LogEntryRecord).new)
      end

      def heartbeat? : Bool
        @entries.empty?
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : Nil
        format.encode(@term, io)
        format.encode(@leader_id, io)
        format.encode(@prev_log_lsn, io)
        format.encode(@prev_log_term, io)
        format.encode(@leader_commit, io)
        format.encode(@entries.size.to_u32, io)
        @entries.each(&.to_io(io, format))
      end

      def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
        term = format.decode(UInt64, io)
        leader_id = format.decode(UInt64, io)
        prev_log_lsn = format.decode(UInt64, io)
        prev_log_term = format.decode(UInt64, io)
        leader_commit = format.decode(UInt64, io)
        count = format.decode(UInt32, io)
        entries = Array(LogEntryRecord).new(count.to_i) do
          LogEntryRecord.from_io(io, format)
        end
        new(term, leader_id, prev_log_lsn, prev_log_term, leader_commit, entries)
      end
    end

    struct AppendEntriesResponse
      getter term : UInt64
      getter follower_id : UInt64
      getter? success : Bool
      getter match_lsn : UInt64
      getter conflict_lsn : UInt64
      getter conflict_term : UInt64

      def initialize(@term : UInt64,
                     @follower_id : UInt64,
                     @success : Bool,
                     @match_lsn : UInt64,
                     @conflict_lsn : UInt64 = 0_u64,
                     @conflict_term : UInt64 = 0_u64)
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : Nil
        format.encode(@term, io)
        format.encode(@follower_id, io)
        io.write_byte(@success ? 1_u8 : 0_u8)
        format.encode(@match_lsn, io)
        format.encode(@conflict_lsn, io)
        format.encode(@conflict_term, io)
      end

      def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
        term = format.decode(UInt64, io)
        follower_id = format.decode(UInt64, io)
        success = (io.read_byte || raise IO::EOFError.new) != 0_u8
        match_lsn = format.decode(UInt64, io)
        conflict_lsn = format.decode(UInt64, io)
        conflict_term = format.decode(UInt64, io)
        new(term, follower_id, success, match_lsn, conflict_lsn, conflict_term)
      end
    end

    struct SnapshotSegmentFile
      STREAM_CHUNK_SIZE = 65_536

      getter vhost : String
      getter queue : String
      getter segment_id : UInt32
      getter relative_path : String
      getter source_mfile : MFile?
      getter source_size : UInt32
      getter staged_path : String?

      def initialize(@vhost : String,
                     @queue : String,
                     @segment_id : UInt32,
                     @relative_path : String,
                     @bytes : Bytes = Bytes.empty,
                     @source_mfile : MFile? = nil,
                     @source_size : UInt32 = 0_u32,
                     @staged_path : String? = nil)
        if @source_size == 0_u32 && !@bytes.empty?
          @source_size = @bytes.size.to_u32
        end
      end

      def byte_size : UInt32
        if !@bytes.empty?
          @bytes.size.to_u32
        elsif mfile = @source_mfile
          Math.min(@source_size.to_i64, mfile.size.to_i64).to_u32
        elsif (path = @staged_path) && File.exists?(path)
          File.size(path).to_u32
        else
          @source_size
        end
      end

      def bytes : Bytes
        return @bytes unless @bytes.empty?
        if (path = @staged_path) && File.exists?(path)
          return File.read(path).to_slice
        end
        if mfile = @source_mfile
          sz = Math.min(@source_size.to_i64, mfile.size.to_i64).to_i
          return Bytes.empty if sz <= 0
          buf = Bytes.new(sz)
          buf.copy_from(mfile.to_slice(0, sz))
          return buf
        end
        Bytes.empty
      end

      def release_lease! : Nil
        if mfile = @source_mfile
          mfile.release_lease
        end
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : Nil
        Protocol.write_string(io, @vhost, format)
        Protocol.write_string(io, @queue, format)
        format.encode(@segment_id, io)
        Protocol.write_string(io, @relative_path, format)
        if mfile = @source_mfile
          actual = Math.min(@source_size.to_i64, mfile.size.to_i64).to_u32
          format.encode(actual, io)
          remaining = actual.to_i64
          offset = 0_i64
          while remaining > 0
            avail = mfile.size.to_i64 - offset
            if avail <= 0
              zeros = Bytes.new(Math.min(remaining, STREAM_CHUNK_SIZE.to_i64).to_i)
              while remaining > 0
                zchunk = Math.min(remaining, zeros.size.to_i64).to_i
                io.write(zeros[0, zchunk])
                remaining -= zchunk
              end
              break
            end
            chunk = Math.min(Math.min(remaining, avail), STREAM_CHUNK_SIZE.to_i64).to_i
            io.write(mfile.to_slice(offset, chunk))
            offset += chunk
            remaining -= chunk
          end
        elsif (path = @staged_path) && File.exists?(path)
          File.open(path, "rb") do |f|
            actual = f.size.to_u32
            format.encode(actual, io)
            if actual > 0
              buf = Bytes.new(Math.min(actual.to_i, STREAM_CHUNK_SIZE))
              rem = actual.to_i64
              while rem > 0
                to_read = Math.min(rem, buf.size.to_i64).to_i
                n = f.read(buf[0, to_read])
                if n == 0
                  zeros = Bytes.new(to_read)
                  io.write(zeros)
                  rem -= to_read
                else
                  io.write(buf[0, n])
                  rem -= n
                end
              end
            end
          end
        else
          format.encode(@bytes.size.to_u32, io)
          io.write(@bytes) unless @bytes.empty?
        end
      end

      def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian, staged_path : String? = nil) : self
        vhost = Protocol.read_string(io, format)
        queue = Protocol.read_string(io, format)
        segment_id = format.decode(UInt32, io)
        relative_path = Protocol.read_string(io, format)
        len = format.decode(UInt32, io)
        if path = staged_path
          Dir.mkdir_p(File.dirname(path))
          File.open(path, "wb") do |f|
            if len > 0
              buf = Bytes.new(Math.min(len.to_i, STREAM_CHUNK_SIZE))
              rem = len.to_i64
              while rem > 0
                chunk = Math.min(rem, buf.size.to_i64).to_i
                io.read_fully(buf[0, chunk])
                f.write(buf[0, chunk])
                rem -= chunk
              end
            end
          end
          new(vhost, queue, segment_id, relative_path, Bytes.empty, staged_path: path, source_size: len)
        else
          bytes = Bytes.new(len)
          io.read_fully(bytes) if len > 0
          new(vhost, queue, segment_id, relative_path, bytes, source_size: len)
        end
      end
    end

    struct InstallSnapshotRequest
      getter term : UInt64
      getter leader_id : UInt64
      getter last_included_lsn : UInt64
      getter last_included_term : UInt64
      getter members : Array(MemberInfo)
      getter control_records : Array(LogEntryRecord)
      getter files : Array(SnapshotSegmentFile)

      def initialize(@term : UInt64,
                     @leader_id : UInt64,
                     @last_included_lsn : UInt64,
                     @last_included_term : UInt64,
                     @members : Array(MemberInfo),
                     @control_records : Array(LogEntryRecord) = Array(LogEntryRecord).new,
                     @files : Array(SnapshotSegmentFile) = Array(SnapshotSegmentFile).new)
      end

      def release_leases! : Nil
        @files.each(&.release_lease!)
      end

      def cleanup_staged_files! : Nil
        @files.each do |f|
          if path = f.staged_path
            File.delete?(path)
          end
        end
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : Nil
        format.encode(@term, io)
        format.encode(@leader_id, io)
        format.encode(@last_included_lsn, io)
        format.encode(@last_included_term, io)
        format.encode(@members.size.to_u32, io)
        @members.each(&.to_io(io, format))
        format.encode(@control_records.size.to_u32, io)
        @control_records.each(&.to_io(io, format))
        format.encode(@files.size.to_u32, io)
        @files.each(&.to_io(io, format))
      end

      def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian, staging_dir : String? = nil) : self
        term = format.decode(UInt64, io)
        leader_id = format.decode(UInt64, io)
        last_included_lsn = format.decode(UInt64, io)
        last_included_term = format.decode(UInt64, io)
        member_count = format.decode(UInt32, io)
        members = Array(MemberInfo).new(member_count.to_i) { MemberInfo.from_io(io, format) }
        ctrl_count = format.decode(UInt32, io)
        control_records = Array(LogEntryRecord).new(ctrl_count.to_i) { LogEntryRecord.from_io(io, format) }
        file_count = format.decode(UInt32, io)
        if sdir = staging_dir
          Dir.mkdir_p(sdir)
        end
        files = Array(SnapshotSegmentFile).new(file_count.to_i) do |idx|
          staged_path = staging_dir.try { |dir| File.join(dir, "file_#{idx}.bin") }
          SnapshotSegmentFile.from_io(io, format, staged_path: staged_path)
        end
        new(term, leader_id, last_included_lsn, last_included_term, members, control_records, files)
      end
    end

    struct InstallSnapshotResponse
      getter term : UInt64
      getter follower_id : UInt64
      getter match_lsn : UInt64

      def initialize(@term : UInt64, @follower_id : UInt64, @match_lsn : UInt64)
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : Nil
        format.encode(@term, io)
        format.encode(@follower_id, io)
        format.encode(@match_lsn, io)
      end

      def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
        term = format.decode(UInt64, io)
        follower_id = format.decode(UInt64, io)
        match_lsn = format.decode(UInt64, io)
        new(term, follower_id, match_lsn)
      end
    end

    module Protocol
      class AuthenticationError < Exception; end

      class ProtocolError < Exception; end

      def self.write_string(io : IO, str : String, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : Nil
        slice = str.to_slice
        format.encode(slice.size.to_u32, io)
        io.write(slice) unless slice.empty?
      end

      def self.read_string(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : String
        len = format.decode(UInt32, io)
        return "" if len == 0
        String.new(len.to_i) do |buf|
          io.read_fully(Slice.new(buf, len.to_i))
          {len.to_i, 0}
        end
      end

      def self.compute_hmac(secret : String, challenge : Bytes, node_id : UInt64) : Bytes
        msg = Bytes.new(challenge.size + sizeof(UInt64))
        challenge.copy_to(msg)
        IO::ByteFormat::LittleEndian.encode(node_id, msg + challenge.size)
        OpenSSL::HMAC.digest(:sha256, secret, msg)
      end

      # Client-side HMAC-SHA256 challenge-response handshake over plain TCP.
      def self.client_handshake!(io : IO, node_id : UInt64, secret : String, peer_uri : String = "") : UInt64
        io.write(MAGIC)
        io.write_byte(PROTOCOL_VERSION)
        io.write_byte(FrameType::HandshakeHello.value)
        IO::ByteFormat::LittleEndian.encode(node_id, io)
        write_string(io, peer_uri)
        io.flush

        frame_byte = io.read_byte || raise IO::EOFError.new
        unless FrameType.from_value?(frame_byte) == FrameType::HandshakeChallenge
          raise ProtocolError.new("Expected HandshakeChallenge, got #{frame_byte}")
        end
        server_node_id = IO::ByteFormat::LittleEndian.decode(UInt64, io)
        server_challenge = Bytes.new(CHALLENGE_SIZE)
        io.read_fully(server_challenge)

        client_challenge = Random::Secure.random_bytes(CHALLENGE_SIZE)
        digest = compute_hmac(secret, server_challenge, node_id)

        io.write_byte(FrameType::HandshakeResponse.value)
        io.write(digest)
        io.write(client_challenge)
        io.flush

        ack_byte = io.read_byte || raise IO::EOFError.new
        unless FrameType.from_value?(ack_byte) == FrameType::HandshakeAck
          raise AuthenticationError.new("Handshake rejected by peer")
        end
        status = io.read_byte || raise IO::EOFError.new
        raise AuthenticationError.new("HMAC verification failed on peer") unless status == 1_u8

        server_digest = Bytes.new(DIGEST_SIZE)
        io.read_fully(server_digest)
        expected_server_digest = compute_hmac(secret, client_challenge, server_node_id)
        unless Crypto::Subtle.constant_time_compare(server_digest, expected_server_digest)
          raise AuthenticationError.new("Mutual HMAC verification failed for server")
        end

        server_node_id
      end

      # Server-side HMAC-SHA256 challenge-response handshake over plain TCP.
      def self.server_handshake!(io : IO, local_node_id : UInt64, secret : String) : Tuple(UInt64, String)
        magic = Bytes.new(4)
        io.read_fully(magic)
        raise ProtocolError.new("Invalid magic header") unless magic == MAGIC

        version = io.read_byte || raise IO::EOFError.new
        raise ProtocolError.new("Unsupported protocol version #{version}") unless version == PROTOCOL_VERSION

        hello_byte = io.read_byte || raise IO::EOFError.new
        unless FrameType.from_value?(hello_byte) == FrameType::HandshakeHello
          raise ProtocolError.new("Expected HandshakeHello, got #{hello_byte}")
        end
        remote_node_id = IO::ByteFormat::LittleEndian.decode(UInt64, io)
        remote_peer_uri = read_string(io)

        server_challenge = Random::Secure.random_bytes(CHALLENGE_SIZE)
        io.write_byte(FrameType::HandshakeChallenge.value)
        IO::ByteFormat::LittleEndian.encode(local_node_id, io)
        io.write(server_challenge)
        io.flush

        resp_byte = io.read_byte || raise IO::EOFError.new
        unless FrameType.from_value?(resp_byte) == FrameType::HandshakeResponse
          raise ProtocolError.new("Expected HandshakeResponse, got #{resp_byte}")
        end
        client_digest = Bytes.new(DIGEST_SIZE)
        io.read_fully(client_digest)
        client_challenge = Bytes.new(CHALLENGE_SIZE)
        io.read_fully(client_challenge)

        expected_client_digest = compute_hmac(secret, server_challenge, remote_node_id)
        unless Crypto::Subtle.constant_time_compare(client_digest, expected_client_digest)
          io.write_byte(FrameType::HandshakeAck.value)
          io.write_byte(0_u8)
          io.flush
          raise AuthenticationError.new("Client HMAC verification failed")
        end

        server_digest = compute_hmac(secret, client_challenge, local_node_id)
        io.write_byte(FrameType::HandshakeAck.value)
        io.write_byte(1_u8)
        io.write(server_digest)
        io.flush

        {remote_node_id, remote_peer_uri}
      end

      def self.write_frame(io : IO, type : FrameType, & : IO -> Nil) : Nil
        io.write_byte(type.value)
        yield io
        io.flush
      end
    end
  end
end

require "../spec_helper"
require "lz4"

# The follower's side of replication protocol version 3 (see WireV3):
# records carry the journal lsn and an append its offset, so applying one is
# idempotent, and acks carry the applied lsn.
module ClientV3Spec
  extend ClusteringSpecHelper
  alias TestClient = ClusteringSpecHelper::TestClient
  alias WireV3 = LavinMQ::Clustering::WireV3
  alias Op = LavinMQ::Clustering::WireV3::Op

  record Stream, client : TestClient, client_socket : FakeSocket, leader_io : UNIXSocket,
    lz4 : Compress::LZ4::Writer, data_dir : String do
    def close
      client_socket.close
      leader_io.close
    end
  end

  # A client streaming v3 records from `leader_io`; `stream_changes` runs in a
  # fiber and `errors` collects what ends it.
  def self.start(data_dir, sync = true, errors = Channel(Exception).new(1)) : Stream
    client = make_client(data_dir, sync: sync, version: 3)
    client_socket, leader_io = FakeSocket.pair
    lz4_reader = Compress::LZ4::Reader.new(client_socket)
    lz4 = Compress::LZ4::Writer.new(leader_io, Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))
    spawn(name: "client v3 stream_changes") do
      client.stream_changes_public(client_socket, lz4_reader)
    rescue ex : IO::Error
      errors.try_send ex
    end
    Stream.new(client, client_socket, leader_io, lz4, data_dir)
  end

  # Reads acks until one reports `lsn` applied; returns its received bytes
  def self.wait_applied(io : IO, lsn : UInt64) : Int64
    io.read_timeout = 2.seconds
    buf = Bytes.new(WireV3::ACK_SIZE)
    loop do
      io.read_fully(buf)
      applied, received = WireV3.decode_ack(buf)
      return received if applied >= lsn
    end
  end

  def self.read_ack(io : IO) : {UInt64, Int64}
    io.read_timeout = 2.seconds
    buf = Bytes.new(WireV3::ACK_SIZE)
    io.read_fully(buf)
    WireV3.decode_ack(buf)
  end

  def self.append(s : Stream, lsn, path, offset, payload : String) : Int64
    size = WireV3.write_record(s.lz4, lsn.to_u64, Op::Append, path, offset.to_i64, payload.to_slice)
    s.lz4.flush
    size
  end

  # The leader's side of a v3 connection up to the resume reply
  def self.accept(leader_io : IO, journal_id : UInt64, resume : Bool) : {UInt64, UInt64}
    header = Bytes.new(8)
    leader_io.read_fully(header)
    header.should eq LavinMQ::Clustering::StartV3
    # Echo the header and challenge the follower, which answers with an HMAC
    challenge = Random::Secure.random_bytes(LavinMQ::Clustering::CHALLENGE_SIZE)
    leader_io.write LavinMQ::Clustering::StartV3 + challenge
    response = Bytes.new(32)
    leader_io.read_fully(response)
    response.should eq LavinMQ::Clustering.challenge_response("password", challenge, LavinMQ::Clustering::StartV3)
    leader_io.write_byte 0u8
    leader_io.read_bytes Int32, IO::ByteFormat::LittleEndian # the follower id
    request = WireV3.read_resume_request(leader_io)
    WireV3.write_resume_reply(leader_io, resume, journal_id)
    request
  end

  def self.position_file(data_dir) : String?
    File.read(File.join(data_dir, LavinMQ::Clustering::Client::POSITION_FILE)).strip
  rescue File::NotFoundError
    nil
  end

  describe LavinMQ::Clustering::Client do
    describe "version 3 appends" do
      it "writes at the given offset and acks the record as applied" do
        with_datadir do |data_dir|
          s = start(data_dir)
          size = append(s, 1, "seg", 0, "head")
          size += append(s, 2, "seg", 4, "tail")
          wait_applied(s.leader_io, 2u64).should eq size
          File.read(File.join(data_dir, "seg")).should eq "headtail"
          s.close
        end
      end

      it "rewrites bytes it already has instead of appending them again" do
        with_datadir do |data_dir|
          s = start(data_dir)
          append(s, 1, "seg", 0, "head")
          append(s, 2, "seg", 4, "part")
          append(s, 3, "seg", 4, "partmore")     # straddles what it has, as after a join cut
          append(s, 4, "seg", 0, "headpartmore") # a full retransmission
          wait_applied(s.leader_io, 4u64)
          File.read(File.join(data_dir, "seg")).should eq "headpartmore"
          s.close
        end
      end

      it "drops the checksum of a file it rewrote part of" do
        with_datadir do |data_dir|
          s = start(data_dir)
          append(s, 1, "seg", 0, "headpart")
          append(s, 2, "seg", 8, "more") # sequential: the running checksum stays valid
          wait_applied(s.leader_io, 2u64)
          close_client(s.client)
          persisted_checksum(data_dir, "seg").should eq Digest::SHA1.hexdigest("headpartmore")

          s2 = start(data_dir) # continues at the position the close persisted
          append(s2, 3, "seg", 4, "XXXXmore")
          wait_applied(s2.leader_io, 3u64) # still applied and acked
          File.read(File.join(data_dir, "seg")).should eq "headXXXXmore"
          close_client(s2.client)
          persisted_checksum(data_dir, "seg").should be_nil
          s2.close
        end
      end

      it "treats an append beyond the end of the file as a protocol error" do
        with_datadir do |data_dir|
          errors = Channel(Exception).new(1)
          s = start(data_dir, errors: errors)
          append(s, 1, "seg", 0, "head")
          wait_applied(s.leader_io, 1u64)
          append(s, 2, "seg", 8, "gap")
          select
          when ex = errors.receive
            ex.should be_a(LavinMQ::Clustering::ProtocolError)
            ex.message.should match /beyond/
          when timeout(2.seconds)
            fail "an append past the end of the file was accepted"
          end
          File.read(File.join(data_dir, "seg")).should eq "head"
          s.close
        end
      end

      it "treats a gap in the lsns as a protocol error" do
        with_datadir do |data_dir|
          errors = Channel(Exception).new(1)
          s = start(data_dir, errors: errors)
          append(s, 1, "seg", 0, "head")
          wait_applied(s.leader_io, 1u64)
          append(s, 3, "seg", 4, "tail")
          select
          when ex = errors.receive
            ex.should be_a(LavinMQ::Clustering::ProtocolError)
            ex.message.should match /Expected journal entry 2/
          when timeout(2.seconds)
            fail "a skipped lsn was accepted"
          end
          s.close
        end
      end

      # A large record is received over time; the leader must see progress
      # (received bytes) while the record's lsn is still not applied
      it "reports received bytes per chunk with the previous record as applied" do
        with_datadir do |data_dir|
          s = start(data_dir)
          append(s, 1, "big", 0, "x")
          wait_applied(s.leader_io, 1u64)
          chunk = LavinMQ::Clustering::Client::BUFFER_SIZE
          payload = Bytes.new(chunk * 3, 0xAB_u8)
          # The record's header promises the whole payload, only a chunk follows
          s.lz4.write_bytes 2u64, IO::ByteFormat::LittleEndian
          s.lz4.write_byte Op::Append.value
          s.lz4.write_bytes "big".bytesize, IO::ByteFormat::LittleEndian
          s.lz4.write "big".to_slice
          s.lz4.write_bytes 1i64, IO::ByteFormat::LittleEndian
          s.lz4.write_bytes payload.size.to_i64, IO::ByteFormat::LittleEndian
          s.lz4.write payload[0, chunk]
          s.lz4.flush

          framing = WireV3.record_size(Op::Append, "big")
          first = WireV3.record_size(Op::Append, "big", 1)
          received = 0i64
          loop do
            applied, received = read_ack(s.leader_io)
            applied.should eq 1u64 # the big record is not applied yet
            break if received >= first + framing + chunk
          end

          s.lz4.write payload[chunk..]
          s.lz4.flush
          wait_applied(s.leader_io, 2u64).should eq first + framing + payload.size
          File.size(File.join(data_dir, "big")).should eq 1 + payload.size
          s.close
        end
      end
    end

    describe "version 3 replace, delete and fsync" do
      it "acks a replace as applied only once the file is renamed into place" do
        with_datadir do |data_dir|
          s = start(data_dir)
          File.write File.join(data_dir, "replaced"), "old content"
          size = WireV3.write_record(s.lz4, 1u64, Op::Replace, "replaced", payload: "new content".to_slice)
          s.lz4.flush
          wait_applied(s.leader_io, 1u64).should eq size
          File.read(File.join(data_dir, "replaced")).should eq "new content"
          File.exists?(File.join(data_dir, "replaced.tmp")).should be_false
          s.close
        end
      end

      it "creates an empty file for an empty replace" do
        with_datadir do |data_dir|
          s = start(data_dir)
          WireV3.write_record(s.lz4, 1u64, Op::Replace, "empty", payload: Bytes.empty)
          s.lz4.flush
          wait_applied(s.leader_io, 1u64)
          File.exists?(File.join(data_dir, "empty")).should be_true
          File.size(File.join(data_dir, "empty")).should eq 0
          s.close
        end
      end

      it "acks a delete once the file is gone" do
        with_datadir do |data_dir|
          s = start(data_dir)
          append(s, 1, "doomed", 0, "data")
          wait_applied(s.leader_io, 1u64)
          size = WireV3.write_record(s.lz4, 2u64, Op::Delete, "doomed")
          s.lz4.flush
          wait_applied(s.leader_io, 2u64).should eq WireV3.record_size(Op::Append, "doomed", 4) + size
          File.exists?(File.join(data_dir, "doomed")).should be_false
          s.close
        end
      end

      it "fsyncs the requested file, and the dir of a file it created, before acking the request" do
        with_datadir do |data_dir|
          s = start(data_dir)
          filename = "vhost/queue/msgs.0000000001"
          append(s, 1, filename, 0, "data")
          WireV3.write_record(s.lz4, 2u64, Op::Fsync, filename)
          s.lz4.flush
          wait_applied(s.leader_io, 2u64)
          s.client.fsynced_paths.should eq [File.join(data_dir, filename), File.join(data_dir, "vhost/queue")]
          s.client.syncs_started.should eq 0

          WireV3.write_record(s.lz4, 3u64, Op::Fsync, "")
          s.lz4.flush
          wait_applied(s.leader_io, 3u64)
          s.client.syncs_started.should eq 1
          s.close
        end
      end
    end

    describe "resuming from the journal" do
      it "records the cut after a full sync and offers it on the next connect" do
        with_datadir do |data_dir|
          client = make_client(data_dir, version: 3)
          client_socket, leader_io = FakeSocket.pair # sync sets TCP options on its socket
          lz4_reader = Compress::LZ4::Reader.new(client_socket)
          lz4 = Compress::LZ4::Writer.new(leader_io, Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))
          journal_id = 42u64
          leader = Channel({UInt64, UInt64}).new(1)
          spawn(name: "full sync leader") do
            leader.send accept(leader_io, journal_id, resume: false)
            2.times do
              lz4.write_bytes 0i32, IO::ByteFormat::LittleEndian # empty file list
              lz4.flush
              leader_io.read_bytes Int32, IO::ByteFormat::LittleEndian # no files requested
            end
            WireV3.write_cut(lz4, 100u64)
            lz4.flush
          end
          client.sync_public(client_socket, lz4_reader)
          leader.receive.should eq({0u64, 0u64}) # nothing to offer yet
          client.full_syncs.should eq 1
          client.persisted_lsn.should eq 99
          client.journal_id.should eq journal_id
          position_file(data_dir).should eq "42 99"
          client_socket.close
          leader_io.close

          # A new client in the same data dir offers that position
          client2 = make_client(data_dir, version: 3)
          client2.persisted_lsn.should eq 99
          client2.journal_id.should eq journal_id
          client_socket, leader_io = FakeSocket.pair # sync sets TCP options on its socket
          lz4_reader = Compress::LZ4::Reader.new(client_socket)
          spawn(name: "resuming leader") { leader.send accept(leader_io, journal_id, resume: true) }
          client2.sync_public(client_socket, lz4_reader)
          leader.receive.should eq({journal_id, 99u64})
          client2.full_syncs.should eq 0
          client_socket.close
          leader_io.close
        end
      end

      it "forgets its position while a full sync is under way" do
        with_datadir do |data_dir|
          File.write File.join(data_dir, LavinMQ::Clustering::Client::POSITION_FILE), "7 50\n"
          client = make_client(data_dir, version: 3)
          client.persisted_lsn.should eq 50
          client_socket, leader_io = FakeSocket.pair # sync sets TCP options on its socket
          lz4_reader = Compress::LZ4::Reader.new(client_socket)
          lz4 = Compress::LZ4::Writer.new(leader_io, Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))
          spawn(name: "stalling leader") do
            accept(leader_io, 7u64, resume: false)
            lz4.write_bytes 0i32, IO::ByteFormat::LittleEndian
            lz4.flush
            leader_io.read_bytes Int32, IO::ByteFormat::LittleEndian
            # the second pass never completes
          end
          spawn(name: "client sync") do
            client.sync_public(client_socket, lz4_reader)
          rescue IO::Error
          end
          wait_for { position_file(data_dir).nil? }
          client.persisted_lsn.should eq 0
          client_socket.close
          leader_io.close
        end
      end

      it "streams from the entry after its position and persists what it applied on close" do
        with_datadir do |data_dir|
          File.write File.join(data_dir, LavinMQ::Clustering::Client::POSITION_FILE), "7 50\n"
          s = start(data_dir)
          # The leader resumes at 51, which the stream must start with
          append(s, 51, "seg", 0, "head")
          append(s, 52, "seg", 4, "tail")
          wait_applied(s.leader_io, 52u64)
          close_client(s.client)
          position_file(data_dir).should eq "7 52"
          s.close
        end
      end

      it "rejects a stream that doesn't start after its position" do
        with_datadir do |data_dir|
          File.write File.join(data_dir, LavinMQ::Clustering::Client::POSITION_FILE), "7 50\n"
          errors = Channel(Exception).new(1)
          s = start(data_dir, errors: errors)
          append(s, 1, "seg", 0, "head")
          select
          when ex = errors.receive
            ex.should be_a(LavinMQ::Clustering::ProtocolError)
            ex.message.should match /Expected journal entry 51/
          when timeout(2.seconds)
            fail "a stream not continuing from the position was accepted"
          end
          s.close
        end
      end
    end

    describe "version negotiation" do
      it "falls back to version 1 when the leader answers with it" do
        with_datadir do |data_dir|
          client = make_client(data_dir, version: 3)
          client_socket, leader_io = UNIXSocket.pair
          spawn(name: "version 1 leader") do
            header = Bytes.new(8)
            leader_io.read_fully(header)
            header.should eq LavinMQ::Clustering::StartV3
            leader_io.write LavinMQ::Clustering::Start
          end
          expect_raises(IO::Error, /version mismatch/) do
            client.authenticate_public(client_socket)
          end
          client.protocol_version.should eq 1
          client_socket.close
        end
      end

      it "reconnects on a leader that rejects its header without offering an older version" do
        with_datadir do |data_dir|
          client = make_client(data_dir, version: 3)
          client_socket, leader_io = UNIXSocket.pair
          spawn(name: "odd leader") do
            header = Bytes.new(8)
            leader_io.read_fully(header)
            leader_io.write Bytes['R'.ord, 'E'.ord, 'P'.ord, 'L'.ord, 'I'.ord, 9, 0, 0]
          end
          expect_raises(LavinMQ::Clustering::ProtocolError) do
            client.authenticate_public(client_socket)
          end
          client.protocol_version.should eq 3
          client_socket.close
        end
      end
    end
  end
end

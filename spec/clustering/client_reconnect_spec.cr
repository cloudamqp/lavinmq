require "../spec_helper"
require "lz4"

# Reproduces the follower state corruption seen after disconnect/resync cycles:
# @files holds open append handles that survive reconnects, while sync_files
# deletes or replaces the files on disk behind them. Streamed appends then land
# in the old unlinked inode (acked but invisible), and a later delete record
# for a reused path raises ENOENT and crash-loops the follower.
module ClientReconnectSpec
  extend ClusteringSpecHelper

  # One streaming "connection": run stream_changes against a socket pair,
  # yield the leader side, then disconnect.
  def self.with_stream_session(client, &)
    client_socket, leader_io = FakeSocket.pair
    lz4_reader = Compress::LZ4::Reader.new(client_socket)
    lz4_writer = Compress::LZ4::Writer.new(leader_io,
      Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))
    done = Channel(Nil).new
    spawn(name: "client stream_changes") do
      client.stream_changes_public(client_socket, lz4_reader)
    rescue IO::Error
      # disconnect
    ensure
      done.send nil
    end
    leader_io.read_timeout = 2.seconds
    yield lz4_writer, leader_io
  ensure
    client_socket.try &.close
    leader_io.try &.close
    done.try &.receive
  end

  # One full-sync "reconnect" against a simulated leader file set.
  def self.run_sync(client, leader_files)
    server_io, client_io = UNIXSocket.pair
    lz4_reader = Compress::LZ4::Reader.new(client_io)
    done = Channel(Nil).new
    spawn do
      simulate_leader(server_io, leader_files, rounds: 2)
      done.send nil
    end
    client.full_sync_public(client_io, lz4_reader)
    select
    when done.receive
    when timeout(1.second)
      fail "leader fiber timed out"
    end
  ensure
    server_io.try &.close
    client_io.try &.close
  end

  def self.send_append(lz4_writer, filename, payload)
    lz4_writer.write_bytes filename.bytesize, IO::ByteFormat::LittleEndian
    lz4_writer.write filename.to_slice
    lz4_writer.write_bytes(-payload.bytesize.to_i64, IO::ByteFormat::LittleEndian)
    lz4_writer.write payload.to_slice
    lz4_writer.flush
  end

  def self.send_delete(lz4_writer, filename)
    lz4_writer.write_bytes filename.bytesize, IO::ByteFormat::LittleEndian
    lz4_writer.write filename.to_slice
    lz4_writer.write_bytes 0i64, IO::ByteFormat::LittleEndian
    lz4_writer.flush
  end

  def self.read_acks(leader_io, expected : Int64) : Int64
    acked = 0i64
    while acked < expected
      acked += leader_io.read_bytes(Int64, IO::ByteFormat::LittleEndian)
    end
    acked
  end

  def self.framing(filename) : Int64
    (sizeof(Int32) + filename.bytesize + sizeof(Int64)).to_i64
  end

  describe LavinMQ::Clustering::Client do
    # A file is appended to (open handle in @files), the connection drops, and
    # the resync replaces the file on disk because its content diverged. The
    # handle in @files still points to the deleted inode, so appends on the
    # next connection must not go through it: they'd be acked as durable but
    # never reach the visible file, and the follower silently diverges again.
    it "applies streamed appends to the file re-fetched during resync, not the stale handle" do
      with_datadir do |data_dir|
        client = make_client(data_dir, sync: false)
        filename = "queue1/msgs.0000000001"
        path = File.join(data_dir, filename)

        with_stream_session(client) do |w, io|
          send_append(w, filename, "AAAA")
          read_acks(io, framing(filename) + 4)
        end
        File.read(path).should eq "AAAA"

        # Leader got more appends while we were disconnected; the resync
        # detects the hash mismatch, deletes our copy and re-fetches it.
        run_sync(client, {filename => "AAAABBBB"})
        File.read(path).should eq "AAAABBBB"

        with_stream_session(client) do |w, io|
          send_append(w, filename, "CCCC")
          read_acks(io, framing(filename) + 4)
        end
        File.read(path).should eq "AAAABBBBCCCC"

        spawn { client.@follower_done.send(nil) }
        client.close
      end
    end

    # A queue is deleted on the leader while the follower is disconnected, so
    # the resync deletes its files locally. The queue is then recreated, so the
    # same paths stream again (segments restart at .0000000001). Appends must
    # recreate the file on disk, and the eventual delete record must apply and
    # be acked instead of raising ENOENT and crash-looping the follower.
    it "handles a path reused after a resync deleted it" do
      with_datadir do |data_dir|
        client = make_client(data_dir, sync: false)
        filename = "queue1/msgs.0000000001"
        path = File.join(data_dir, filename)

        with_stream_session(client) do |w, io|
          send_append(w, filename, "AAAA")
          read_acks(io, framing(filename) + 4)
        end

        # Queue deleted on leader while disconnected: not in the leader's file
        # list, so the resync deletes it locally.
        run_sync(client, {} of String => String)
        File.exists?(path).should be_false

        # Queue recreated: same path streams again, then the segment is deleted.
        with_stream_session(client) do |w, io|
          send_append(w, filename, "BBBB")
          read_acks(io, framing(filename) + 4)
          File.read(path).should eq "BBBB"
          send_delete(w, filename)
          read_acks(io, framing(filename))
          File.exists?(path).should be_false
        end

        spawn { client.@follower_done.send(nil) }
        client.close
      end
    end

    # A leader that fails the handshake, here one with another password,
    # mustn't end replication for good: the controller still thinks this
    # client follows it and won't replace it while that node leads. Both
    # sides log each attempt, so the client waits longer each time.
    it "keeps reconnecting, waiting longer each time, to a leader that fails the handshake" do
      with_datadir do |data_dir|
        client = make_client(data_dir, sync: false)
        leader = TCPServer.new("127.0.0.1", 0)
        accepted = Channel(Time::Instant).new(16)
        spawn(name: "leader with another password") do
          while socket = leader.accept?
            accepted.send Time.instant # before the handshake the client fails
            begin
              # Read all the follower sends, so closing doesn't reset the connection
              socket.read_fully(Bytes.new(LavinMQ::Clustering::StartV2.size))
              socket.read_fully(Bytes.new(socket.read_byte || raise IO::EOFError.new))
              socket.write_byte 1u8 # wrong password
            rescue IO::Error
            ensure
              socket.close
            end
          end
        end
        spawn(name: "follow") { client.follow("127.0.0.1", leader.local_address.port) }
        connects = Array(Time::Instant).new
        3.times do
          select
          when instant = accepted.receive
            connects << instant
          when timeout(5.seconds)
            fail "stopped reconnecting after #{connects.size} connection(s)"
          end
        end
        # It waits 1s after the first failure, then 2s after the second, which
        # comes after the second connect was accepted, so this is never shorter
        (connects[2] - connects[1]).should be >= 1.5.seconds
      ensure
        client.try &.close
        leader.try &.close
      end
    end
  end
end

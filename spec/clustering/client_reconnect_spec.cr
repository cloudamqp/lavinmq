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

  # A port that nothing listens on, so connecting to it is refused
  def self.closed_port : Int32
    TCPServer.open("127.0.0.1", 0, &.local_address.port)
  end

  # Holds the follow loop at a gate, either before it binds anything or after
  # it bound the control socket, so a close can run at that point
  class GatedClient < ClusteringSpecHelper::TestClient
    getter gate = Channel(Nil).new
    property? hold_before_bind = true

    private def follow_loop(leader : LavinMQ::Clustering::Client::Leader)
      @gate.receive if hold_before_bind?
      super
    end

    private def hash_local_files : Nil
      @gate.receive unless hold_before_bind?
      super
    end
  end

  # Follows a non-loopback leader, so the loop binds the control socket, closes
  # while the loop is held at the gate, and checks that close unbound the socket
  def self.close_while_held(data_dir, socket_path, hold_before_bind)
    config = LavinMQ::Config.instance.dup
    config.data_dir = data_dir
    config.sync = false
    config.metrics_http_port = -1
    client = GatedClient.new(config, 1, "password", proxy: false)
    client.hold_before_bind = hold_before_bind
    spawn { client.follow("192.0.2.1", closed_port) }
    Fiber.yield # the loop runs up to the gate
    File.exists?(socket_path).should eq !hold_before_bind
    closed = Channel(Exception?).new
    spawn do
      client.close
      closed.send nil
    rescue ex
      closed.send ex
    end
    Fiber.yield # close waits for the loop
    client.gate.send nil
    select
    when ex = closed.receive then raise ex if ex
    when timeout(5.seconds) then fail "close didn't return"
    end
    expect_raises(Socket::ConnectError) { UNIXSocket.new(socket_path) }
  ensure
    if (server = client.try &.@internal_http_server) && !server.closed?
      server.close
    end
  end

  def self.with_control_socket_path(&)
    config = LavinMQ::Config.instance
    original_path = config.control_unix_path
    socket_path = File.tempname("lavinmqctl-spec", ".sock")
    config.control_unix_path = socket_path
    yield socket_path
  ensure
    LavinMQ::Config.instance.control_unix_path = original_path if original_path
    File.delete?(socket_path) if socket_path
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

        client.close
      end
    end

    it "closes at once when it never started following, and doesn't follow after" do
      with_datadir do |data_dir|
        client = make_client(data_dir, sync: false)
        Time.measure { client.close }.should be < 1.second

        followed = Channel(Nil).new
        spawn { client.follow("127.0.0.1", closed_port); followed.send nil }
        select
        when followed.receive
        when timeout(1.second) then fail "follow didn't return after close"
        end
      end
    end

    it "unbinds the control socket when closed after the follow loop bound it" do
      with_datadir do |data_dir|
        with_control_socket_path do |socket_path|
          close_while_held(data_dir, socket_path, hold_before_bind: false)
        end
      end
    end

    it "unbinds the control socket when closed before the follow loop bound it" do
      with_datadir do |data_dir|
        with_control_socket_path do |socket_path|
          close_while_held(data_dir, socket_path, hold_before_bind: true)
        end
      end
    end

    it "cuts the reconnect backoff short when closed" do
      with_datadir do |data_dir|
        client = make_client(data_dir, sync: false)
        spawn { client.follow("127.0.0.1", closed_port) }
        sleep 100.milliseconds # the connect is refused at once, then it backs off for 1s
        Time.measure { client.close }.should be < 500.milliseconds
      end
    end
  end
end

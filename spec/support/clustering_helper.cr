require "lz4"
require "../../src/lavinmq/clustering/client"
require "../../src/lavinmq/clustering/server"

# A TCP port that was free a moment ago
def free_port : Int32
  s = TCPServer.new("127.0.0.1", 0)
  s.local_address.port
ensure
  s.try &.close
end

# The follower's side of a version 3 replication handshake: the start
# header, the leader's echoed header and challenge, our HMAC, the leader's
# accept byte and the follower id. Without `accepted` it stops after the
# HMAC, for the caller to read the leader's refusal.
def clustering_handshake(io : IO, password : String, header : Bytes, id : Int32, accepted = true) : Nil
  io.write header
  echo = Bytes.new(8)
  io.read_fully(echo)
  echo.should eq header
  challenge = Bytes.new(LavinMQ::Clustering::CHALLENGE_SIZE)
  io.read_fully(challenge)
  io.write LavinMQ::Clustering.challenge_response(password, challenge, header)
  io.flush
  return unless accepted
  io.read_byte.should eq 0u8
  io.write_bytes id, IO::ByteFormat::LittleEndian
  io.flush
end

# Drives the clustering handshake (version 1 or 3 by `header`) and the two
# full-sync passes (requesting no files) so the follower reaches the Synced
# state on the leader. Returns the socket and the LZ4 reader positioned at
# the start of the change stream.
def sync_fake_follower(server, port, id : Int32, header = LavinMQ::Clustering::Start) : {TCPSocket, Compress::LZ4::Reader}
  io = TCPSocket.new("localhost", port)
  v3 = LavinMQ::Clustering.header_version(header) == 3
  if v3
    clustering_handshake(io, server.password, header, id)
    # offer no position: a full sync follows
    LavinMQ::Clustering::WireV3.write_resume_request(io, 0u64, 0u64)
    LavinMQ::Clustering::WireV3.read_resume_reply(io)[0].should be_false
  else
    io.write header
    io.write_bytes server.password.bytesize.to_u8, IO::ByteFormat::LittleEndian
    io.write server.password.to_slice
    io.read_byte # password-accepted byte
    io.write_bytes id, IO::ByteFormat::LittleEndian
    io.flush
  end
  lz4 = Compress::LZ4::Reader.new(io)
  sha1_size = Digest::SHA1.new.digest_size
  2.times do
    loop do
      len = lz4.read_bytes Int32, IO::ByteFormat::LittleEndian
      break if len.zero?
      lz4.skip len
      lz4.skip sha1_size
    end
    io.write_bytes 0i32 # request no files
    io.flush
  end
  LavinMQ::Clustering::WireV3.read_cut(lz4) if v3
  {io, lz4}
end

# A node of a raft cluster of this process: its own raft and replication
# ports on localhost, a short election timeout, every client listener on a
# free port and nothing else bound. Alone in the cluster unless given seeds.
def raft_spec_config(data_dir : String, raft_port = free_port, seeds = "127.0.0.1:#{raft_port}",
                     bootstrap = false, election_timeout = 300) : LavinMQ::Config
  config = LavinMQ::Config.new
  config.data_dir = data_dir
  config.clustering = true
  config.clustering_backend = LavinMQ::ClusteringBackend::Raft
  config.clustering_bind = "127.0.0.1"
  config.clustering_raft_port = raft_port
  config.clustering_raft_advertised_address = "127.0.0.1:#{raft_port}"
  config.clustering_seeds = seeds
  config.clustering_bootstrap = bootstrap
  config.clustering_secret = "raft-spec"
  config.clustering_election_timeout = election_timeout
  config.clustering_heartbeat_interval = election_timeout // 6
  config.clustering_port = free_port
  config.clustering_advertised_uri = "tcp://127.0.0.1:#{config.clustering_port}"
  config.amqp_bind = config.http_bind = config.mqtt_bind = "127.0.0.1"
  config.amqp_port = config.http_port = config.mqtt_port = 0
  config.amqps_port = config.https_port = config.mqtts_port = -1
  config.unix_path = config.http_unix_path = config.mqtt_unix_path = ""
  config.metrics_http_port = -1
  config
end

# Shared by the clustering client specs; `extend` it in the spec module.
module ClusteringSpecHelper
  class TestClient < LavinMQ::Clustering::Client
    def sync_files_public(socket, lz4)
      sync_files(socket, lz4)
    end

    def stream_changes_public(socket, lz4)
      stream_changes(socket, lz4)
    end

    def hash_local_files_public
      hash_local_files
    end

    def full_sync_public(socket, lz4)
      full_sync(socket, lz4)
    end

    # Lets specs assert that nothing is hashed while the leader is connected.
    getter files_hashed = 0
    # Simulates an unreadable file; permissions can't, specs may run as root.
    property fail_hash_for : String? = nil

    private def hash_file(filename : String, path : String) : Bytes
      @files_hashed += 1
      raise File::Error.new("Permission denied", file: path) if filename == @fail_hash_for
      super
    end

    # Mirrors what #sync does on a reconnect: drop the state describing the data
    # dir as the previous connection left it, then compare against the leader.
    def resync_files_public(socket, lz4)
      reset_file_state
      sync_files(socket, lz4)
    end

    # Instrumentation for the close/ack-loop fd race spec: slow each sync down
    # and record whether one ever ran against a closed data dir fd — the real
    # implementation would Log.fatal and exit 1 there.
    property sync_delay : Time::Span = Time::Span.zero
    getter syncs_started = 0
    getter? synced_on_closed_fd = false

    # Instrumentation for the log-loop lifecycle spec: how many streamed-bytes
    # logging fibers are currently running.
    getter log_loops_running = 0

    private def log_streamed_bytes_loop(done : Channel(Nil))
      @log_loops_running += 1
      super
    ensure
      @log_loops_running -= 1
    end

    # Paths fsynced on the leader's fsync requests, in order
    getter fsynced_paths = Array(String).new

    def protocol_version=(@protocol_version : Int32)
    end

    def protocol_version : Int32
      @protocol_version
    end

    def authenticate_public(socket)
      authenticate(socket)
    end

    def sync_public(socket, lz4)
      sync(socket, lz4)
    end

    def persisted_lsn : UInt64
      @persisted_lsn
    end

    def journal_id : UInt64
      @journal_id
    end

    private def fsync_path(path : String) : Nil
      @fsynced_paths << path
      super
    end

    private def sync_data_dir : Nil
      @syncs_started += 1
      sleep @sync_delay unless @sync_delay.zero?
      if LibC.fcntl(@data_dir_fd, LibC::F_GETFD, 0) == -1
        @synced_on_closed_fd = true
        return
      end
      super
    end
  end

  # follow() was never called in this harness, so satisfy close's follower-done
  # handshake ourselves.
  def close_client(client : TestClient)
    spawn(name: "follower done feeder") { client.@follower_done.send(nil) }
    client.close
  end

  # What checksums.sha1 holds: filename => {hash, size}
  def persisted_checksums(data_dir : String) : Hash(String, {String, String})
    path = File.join(data_dir, "checksums.sha1")
    return Hash(String, {String, String}).new unless File.exists?(path)
    File.read_lines(path).to_h do |line|
      hash, _, rest = line.partition(" ")
      size, _, filename = rest.partition(" *")
      {filename, {hash, size}} # a later line wins, as in Checksums#restore
    end
  end

  def persisted_checksum(data_dir : String, filename : String) : String?
    persisted_checksums(data_dir)[filename]?.try &.first
  end

  # `version` is the replication protocol the client streams with; the v1
  # wire-format specs predate v3 and drive stream_changes directly.
  def make_client(data_dir : String, sync = true, version = 1, backend = LavinMQ::ClusteringBackend::Etcd) : TestClient
    config = LavinMQ::Config.instance.dup
    config.data_dir = data_dir
    config.sync = sync
    config.clustering_backend = backend
    config.metrics_http_port = -1
    TestClient.new(config, 1, "password", proxy: false).tap &.protocol_version = version
  end

  # Serve `rounds` passes of the file sync protocol (full_sync runs two).
  # Returns every file requested, across all rounds.
  def simulate_leader(io : IO, leader_files : Hash(String, String), rounds = 1)
    lz4 = Compress::LZ4::Writer.new(io, Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))
    all_requested = Array(String).new
    rounds.times do
      leader_files.each do |filename, content|
        hash = Digest::SHA1.digest(content)
        lz4.write_bytes filename.bytesize, IO::ByteFormat::LittleEndian
        lz4.write filename.to_slice
        lz4.write hash
      end
      lz4.write_bytes 0i32, IO::ByteFormat::LittleEndian
      lz4.flush

      # Per round: only this round's requests may be served, or a later round
      # re-sends earlier payloads and the follower reads the wrong file.
      requested = Array(String).new
      loop do
        len = io.read_bytes Int32, IO::ByteFormat::LittleEndian
        break if len == 0
        requested << io.read_string(len)
      end

      requested.each do |filename|
        content = leader_files[filename]? || ""
        lz4.write_bytes content.bytesize.to_i64, IO::ByteFormat::LittleEndian
        lz4.write content.to_slice
        lz4.flush
      end
      all_requested.concat requested
    end
    all_requested
  end
end

class SpecClustering
  getter replicator, config, follower_config, repli
  # The leader's replication port, for a second follower
  getter port : Int32

  @follower_stopped = WaitGroup.new(1)
  @follower_config : LavinMQ::Config
  @stopped = false

  def initialize(@config : LavinMQ::Config, follower_data_dir : String)
    @replicator = LavinMQ::Clustering::Server.new(config, NullCoordinator.new, 0)
    tcp_server = TCPServer.new("localhost", 0)
    @port = tcp_server.local_address.port

    @follower_config = @config.dup.tap &.data_dir = follower_data_dir
    @repli = LavinMQ::Clustering::Client.new(@follower_config, 1, replicator.password, proxy: false)

    spawn(replicator.listen(tcp_server), name: "repli server spec")
    spawn(name: "follow spec") do
      @repli.follow("localhost", tcp_server.local_address.port)
      @follower_stopped.done
    end

    until replicator.followers.size == 1
      Fiber.yield
    end
  end

  def stop
    return if @stopped
    @stopped = true
    @replicator.close
    @repli.close
    @follower_stopped.wait
  end
end

def with_clustering(config = LavinMQ::Config.instance, &)
  follower_data_dir = File.tempname
  Dir.mkdir follower_data_dir
  config = config.dup
  config.metrics_http_port = 0
  yield clustering = SpecClustering.new(config, follower_data_dir)
ensure
  clustering.stop if clustering
  FileUtils.rm_rf follower_data_dir if follower_data_dir
end

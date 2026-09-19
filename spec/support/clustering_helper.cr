require "lz4"
require "../../src/lavinmq/clustering/client"
require "../../src/lavinmq/clustering/server"
require "../../src/lavinmq/clustering/etcd_coordinator"

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

  def make_client(data_dir : String, sync = true) : TestClient
    config = LavinMQ::Config.instance.dup
    config.data_dir = data_dir
    config.sync = sync
    config.metrics_http_port = -1
    TestClient.new(config, 1, "password", proxy: false)
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

  @follower_stopped = WaitGroup.new(1)
  @follower_config : LavinMQ::Config
  @stopped = false

  def initialize(@config : LavinMQ::Config, follower_data_dir : String)
    @replicator = LavinMQ::Clustering::Server.new(config, NullCoordinator.new, 0)
    tcp_server = TCPServer.new("localhost", 0)

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

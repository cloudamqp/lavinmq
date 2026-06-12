require "http/client"
require "ini"
require "json"
require "uri"
require "file_utils"
require "../lavinmq/data_dir_lock"

class LavinMQCtl
  class CtlExit < Exception
    getter code : Int32

    def initialize(@code : Int32, message : String = "")
      super(message)
    end
  end

  RAFT_STATE_DIRS = %w[raft raft-transport]
  # `.clustering_id` is intentionally NOT wiped: it's the container's
  # stable identity, not part of the membership state. Re-joining a
  # cluster with the same id avoids polluting Prometheus TSDB with new
  # series on every reset/rejoin cycle. To get a truly fresh identity,
  # delete the data volume.
  RAFT_STATE_FILES = [] of String

  def raft_status
    response = http.get("/raft/status", @headers)
    if response.status_code != 200
      @io.puts "raft_status: HTTP #{response.status_code}"
      @io.puts response.body
      raise CtlExit.new(1)
    end
    data = JSON.parse(response.body)
    @io.puts "Role:         #{data["role"]?}"
    @io.puts "Term:         #{data["term"]?}"
    @io.puts "Node id:      #{data["id"]?}"
    @io.puts "Leader id:    #{data["leader_id"]?}"
    @io.puts "Commit idx:   #{data["commit_index"]?}"
    @io.puts "Last log:     #{data["last_log_index"]?}"
    if peers = data["peers"]?
      @io.puts "Peers:"
      peers.as_a.each do |p|
        @io.puts "  id=#{p["id"]?} role=#{p["role"]?}"
      end
    end
  rescue ex : IO::Error | Socket::Error
    @io.puts "raft_status: cannot reach local node: #{ex.message}"
    raise CtlExit.new(1)
  end

  # Resolves data_dir using the same precedence as the lavinmq server:
  # CLI --data-dir → ENV LAVINMQ_DATADIR → [main] data_dir in the INI →
  # default /var/lib/lavinmq. Lets `lavinmqctl raft_*` work without
  # repeating --data-dir when a config file is present.
  private def resolved_data_dir : String
    if cli = @options["data_dir"]?
      return cli unless cli.empty?
    end
    if env = ENV["LAVINMQ_DATADIR"]?
      return env unless env.empty?
    end
    config_dir = ENV.fetch("LAVINMQ_CONFIGURATION_DIRECTORY") { ENV.fetch("CONFIGURATION_DIRECTORY", "/etc/lavinmq") }
    ini_path = File.join(config_dir, "lavinmq.ini")
    if File.file?(ini_path)
      begin
        ini = INI.parse(File.read(ini_path))
        if main = ini["main"]?
          if v = main["data_dir"]?
            return v unless v.empty?
          end
        end
      rescue ::INI::ParseException
        # Fall through to the default — surfacing a parse error from a
        # ctl command would mask the actual user intent.
      end
    end
    "/var/lib/lavinmq"
  end

  def raft_reset
    with_wiped_raft_state(resolved_data_dir) { }
  end

  # Wipes the raft state directories while holding the data dir lock, then
  # yields (still locked) for follow-up work like writing `.join_target` —
  # so a systemd-restarted server can't race past its bootstrap-or-join
  # decision before the marker exists. A running node is detected through
  # the same flock the server holds for its whole lifetime; no pidfile
  # configuration needed.
  private def with_wiped_raft_state(data_dir : String, &) : Nil
    Dir.mkdir_p(data_dir)
    lock = LavinMQ::DataDirLock.new(data_dir)
    stop_running_node(lock) unless lock.try_acquire
    begin
      deleted = [] of String
      RAFT_STATE_DIRS.each do |d|
        path = File.join(data_dir, d)
        if Dir.exists?(path)
          FileUtils.rm_rf(path)
          deleted << "#{d}/"
        end
      end
      RAFT_STATE_FILES.each do |f|
        path = File.join(data_dir, f)
        if File.exists?(path)
          File.delete(path)
          deleted << f
        end
      end
      @io.puts "raft_reset: removed #{deleted.empty? ? "(nothing to remove)" : deleted.join(", ")} from #{data_dir}"
      yield
    ensure
      lock.release
    end
  end

  # The data dir lock is held: a node is running. Refuse unless that is
  # provably safe to discard (or --force), then stop the node and wait for
  # its lock to be released. Never wipes under a holder it could not stop.
  private def stop_running_node(lock : LavinMQ::DataDirLock) : Nil
    ensure_safe_to_stop
    info = lock.holder_info
    if m = info.match(/PID (\d+) @ (.+)/)
      pid, host = m[1].to_i64, m[2]
      unless host == System.hostname
        @io.puts "raft_reset: data dir is locked by #{info.inspect} on another host; stop that node first"
        raise CtlExit.new(1)
      end
      @io.puts "raft_reset: node is running, sending SIGTERM to pid #{pid}"
      begin
        Process.signal(Signal::TERM, pid)
      rescue
        @io.puts "raft_reset: could not signal pid #{pid}; stop the node manually"
        raise CtlExit.new(1)
      end
      deadline = Time.instant + 30.seconds
      until lock.try_acquire
        if Time.instant > deadline
          @io.puts "raft_reset: timed out waiting for the node to exit"
          raise CtlExit.new(1)
        end
        sleep 100.milliseconds
      end
    else
      @io.puts "raft_reset: data dir is locked by a running node (#{info.inspect}); stop it first"
      raise CtlExit.new(1)
    end
  end

  # Refuse unless the running node is provably safe to discard: the leader
  # of a single-node cluster (the bootstrap-then-join formation flow). A
  # follower's control socket answers 503, an unreachable socket proves
  # nothing — both require --force.
  private def ensure_safe_to_stop : Nil
    return if @options["force"]?
    begin
      response = http.get("/raft/status", @headers)
      if response.status_code == 200
        data = JSON.parse(response.body)
        peers = data["peers"]?.try(&.as_a.size) || 0
        return if peers <= 1
        @io.puts "raft_reset: refusing — node is in a multi-peer cluster (peers=#{peers}). Use --force to override."
      else
        @io.puts "raft_reset: refusing — node is running but its role can't be verified " \
                 "(HTTP #{response.status_code}; a follower answers 503). " \
                 "Use --force to discard this node's cluster membership."
      end
    rescue ex : IO::Error | Socket::Error
      @io.puts "raft_reset: refusing — node is running but /raft/status is unreachable (#{ex.message}). Use --force to override."
    end
    raise CtlExit.new(1)
  end

  def raft_join
    leader_uri = ARGV.shift? || ""
    if leader_uri.empty?
      @io.puts "raft_join: missing <leader-http-uri>"
      raise CtlExit.new(1)
    end
    parsed = URI.parse(leader_uri)
    unless parsed.scheme == "http" || parsed.scheme == "https"
      @io.puts "raft_join: invalid URI scheme: #{leader_uri}"
      raise CtlExit.new(1)
    end
    data_dir = resolved_data_dir
    with_wiped_raft_state(data_dir) do
      # Written while the data dir lock is still held, so a restarting
      # server can't observe "wiped but no marker" and auto-bootstrap.
      marker = File.join(data_dir, ".join_target")
      File.write(marker, leader_uri)
      @io.puts "raft_join: wrote #{marker} → #{leader_uri}; restart lavinmq to join the cluster"
    end
  end
end

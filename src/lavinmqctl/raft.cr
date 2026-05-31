require "http/client"
require "json"
require "uri"
require "file_utils"

class LavinMQCtl
  class CtlExit < Exception
    getter code : Int32

    def initialize(@code : Int32, message : String = "")
      super(message)
    end
  end

  RAFT_STATE_DIRS  = %w[raft raft-transport]
  RAFT_STATE_FILES = %w[.clustering_id]

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

  # Implemented in Task 9 (file wipe) and Task 10 (running-node detection).
  def raft_reset
    data_dir = @options["data_dir"]? || raise "data_dir not specified (use --data-dir=PATH)"
    # Running-node detection: Task 10 implements maybe_signal_running_node. For now, no-op when no pidfile.
    if pidfile = @options["pidfile"]?
      maybe_signal_running_node(pidfile, data_dir)
    end
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
  end

  private def maybe_signal_running_node(pidfile : String, data_dir : String) : Nil
    return unless File.exists?(pidfile)
    pid_str = File.read(pidfile).strip
    pid = pid_str.to_i64? || return
    return unless Process.exists?(pid)
    unless @options["force"]?
      # Refuse if the node is part of a real cluster.
      # If /raft/status is unreachable assume safe (single-node or already wedged).
      begin
        response = http.get("/raft/status", @headers)
        if response.status_code == 200
          data = JSON.parse(response.body)
          if peers = data["peers"]?
            if peers.as_a.size > 1
              @io.puts "raft_reset: refusing — node is in a multi-peer cluster (peers=#{peers.as_a.size}). Use --force to override."
              raise CtlExit.new(1)
            end
          end
        end
      rescue ex : IO::Error | Socket::Error
        # /raft/status unreachable: skip the safety check.
      end
    end
    @io.puts "raft_reset: sending SIGTERM to pid #{pid}"
    Process.signal(Signal::TERM, pid)
    deadline = Time.instant + 30.seconds
    while Process.exists?(pid)
      if Time.instant > deadline
        @io.puts "raft_reset: timed out waiting for pid #{pid} to exit"
        raise CtlExit.new(1)
      end
      sleep 100.milliseconds
    end
  end

  # Implemented in Task 11.
  def raft_join
    raise "raft_join not implemented"
  end
end

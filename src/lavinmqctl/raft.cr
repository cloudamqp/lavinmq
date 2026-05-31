require "http/client"
require "json"
require "uri"
require "file_utils"

class LavinMQCtl
  RAFT_STATE_DIRS  = %w[raft raft-transport]
  RAFT_STATE_FILES = %w[.clustering_id]

  def raft_status
    response = http.get("/raft/status", @headers)
    if response.status_code != 200
      @io.puts "raft_status: HTTP #{response.status_code}"
      @io.puts response.body
      exit 1
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
    exit 1
  end

  # Implemented in Task 9 (file wipe) and Task 10 (running-node detection).
  def raft_reset
    raise "raft_reset not implemented"
  end

  # Implemented in Task 11.
  def raft_join
    raise "raft_join not implemented"
  end
end

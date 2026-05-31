require "http/client"
require "json"
require "uri"
require "file_utils"

class LavinMQCtl
  RAFT_STATE_DIRS  = %w[raft raft-transport]
  RAFT_STATE_FILES = %w[.clustering_id]

  # Implemented in Task 8.
  def raft_status
    raise "raft_status not implemented"
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

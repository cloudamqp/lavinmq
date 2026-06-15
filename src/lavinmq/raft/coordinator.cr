require "../clustering/coordinator"
require "./server"
require "./cluster_command"
require "random/secure"

module LavinMQ::Raft
  class Coordinator < ::LavinMQ::Clustering::Coordinator
    Log = LavinMQ::Log.for "raft.coordinator"

    PASSWORD_FILE = ".clustering_password"

    def initialize(@server : Server)
    end

    def update_isr(synced_node_ids : Enumerable(Int32)) : Nil
      @server.propose(ClusterCommand::SetIsr.new(synced_node_ids.to_set))
    end

    # The cluster's shared replication secret. It is read from a local file
    # (`<data_dir>/.clustering_password`), never the raft log — replicating it
    # through consensus would persist the secret in every node's log/snapshot.
    #
    # A leader with no file yet generates one (single-node bootstrap); it must
    # be copied to every other node before they join. A node that is not the
    # leader and has no file cannot guess the secret, so it fails fast with an
    # actionable message rather than authenticating followers with the wrong
    # password.
    def password : String
      path = File.join(@server.data_dir, PASSWORD_FILE)
      return File.read(path).strip if File.exists?(path)
      unless @server.is_leader.value
        Log.fatal { "Replication secret file missing: #{path}. Copy it from another node in the cluster." }
        exit 3
      end
      secret = Random::Secure.base64(32)
      File.open(path, "w", perm: 0o600, &.print(secret))
      Log.info { "Generated clustering password at #{path}; copy it to every other node before they join" }
      secret
    end
  end
end

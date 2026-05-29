require "../clustering/coordinator"
require "./server"
require "./cluster_command"
require "random/secure"

module LavinMQ::Raft
  class Coordinator < ::LavinMQ::Clustering::Coordinator
    def initialize(@server : Server)
    end

    def update_isr(synced_node_ids : Set(UInt64)) : Nil
      @server.propose(ClusterCommand::SetIsr.new(synced_node_ids))
    end

    def password : String
      existing = @server.secret
      return existing unless existing.empty?
      new_secret = Random::Secure.base64(32)
      @server.propose(ClusterCommand::SetSecret.new(new_secret))
      deadline = Time.instant + 5.seconds
      while @server.secret.empty?
        raise "timed out waiting for SetSecret apply" if Time.instant > deadline
        Fiber.yield
      end
      @server.secret
    end
  end
end

require "../coordinator"
require "../../config"

module LavinMQ
  module Clustering
    module VR
      # The Coordinator backed by Viewstamped Replication. With etcd gone, the
      # in-sync set and leader election are handled by the VR::Node + control
      # mesh + majority-quorum commit, so the only thing the Server still needs
      # from a Coordinator is the shared replication secret, which is now
      # operator-configured (there is no shared store to mint one).
      class Coordinator < Clustering::Coordinator
        def initialize(@config : Config)
        end

        def password : String
          secret = @config.clustering_secret
          if secret.nil? || secret.empty?
            raise Error.new("clustering_secret must be set when clustering is enabled")
          end
          secret
        end
      end
    end
  end
end

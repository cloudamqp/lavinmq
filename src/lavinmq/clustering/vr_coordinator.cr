require "./coordinator"
require "./password_store"

module LavinMQ
  module Clustering
    class VRCoordinator < Coordinator
      def initialize(@config : Config)
        @password_store = PasswordStore.new(@config.data_dir)
      end

      def update_isr(synced_node_ids : Set(Int32)) : Nil
        synced_node_ids
      end

      def password : String
        @password_store.password(@config.clustering_secret)
      end
    end
  end
end

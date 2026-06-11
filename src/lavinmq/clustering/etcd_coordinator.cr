require "./coordinator"
require "../etcd"
require "../config"
require "random/secure"

module LavinMQ::Clustering
  class EtcdCoordinator < Coordinator
    Log = LavinMQ::Log.for "clustering.etcd_coordinator"

    def initialize(@config : Config, @etcd : Etcd)
    end

    def update_isr(synced_node_ids : Enumerable(Int32)) : Nil
      key = "#{@config.clustering_etcd_prefix}/isr"
      ids = synced_node_ids.map(&.to_s(36)).join(",")
      @etcd.put(key, ids)
    end

    def password : String
      key = "#{@config.clustering_etcd_prefix}/clustering_secret"
      secret = Random::Secure.base64(32)
      stored = @etcd.put_or_get(key, secret)
      Log.info { "Generated new clustering secret" } if stored == secret
      stored
    end
  end
end

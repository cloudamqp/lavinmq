require "../config"
require "../etcd"
require "./controller"

module LavinMQ
  module Clustering
    # Reads and writes this region's `{prefix}/upstream_etcd` key — the durable
    # source of truth for whether the region is a primary or a DR follower of a
    # foreign region (see Controller#effective_upstream). Backs the HTTP API and
    # `lavinmqctl` promotion/demotion commands so failover no longer requires
    # manual etcdctl surgery.
    #
    # It talks to the LOCAL region's etcd, so it works on a relay node even while
    # the foreign primary is down (the failover case). `fail_fast: false` is used
    # so a transient local-etcd outage surfaces as an Etcd::Error to the API
    # caller rather than exiting the process.
    module DRControl
      extend self

      def key : String
        "#{Config.instance.clustering_etcd_prefix}/upstream_etcd"
      end

      # Current DR role: the raw key value, the effective upstream after applying
      # the "primary" sentinel and the config fallback, and whether the region is
      # a primary. Mirrors Controller#effective_upstream.
      def status
        raw = local_etcd.get(key)
        effective = raw || Config.instance.clustering_upstream_etcd_endpoints
        effective = "" if effective == Controller::UPSTREAM_PRIMARY
        {upstream_etcd: raw, effective_upstream: effective, primary: effective.empty?}
      end

      # Set the key. Pass Controller::UPSTREAM_PRIMARY ("primary") to promote this
      # region, or a foreign region's etcd endpoints to demote it to a DR follower.
      def set(value : String) : Nil
        local_etcd.put(key, value)
      end

      # Remove the key, reverting to the config-file fallback
      # (clustering_upstream_etcd_endpoints).
      def clear : Nil
        local_etcd.del(key)
      end

      private def local_etcd : Etcd
        Etcd.new(Config.instance.clustering_etcd_endpoints, fail_fast: false)
      end
    end
  end
end

require "./coordinator"
require "../etcd"
require "../config"
require "random/secure"
require "sync/exclusive"

module LavinMQ::Clustering
  class EtcdCoordinator < Coordinator
    Log = LavinMQ::Log.for "clustering.etcd_coordinator"

    # Reads happen on every update_isr (cold path: follower join/leave) but are
    # already serialized by Clustering::Server's @lock; the write happens once
    # per leadership term. No concurrent readers to parallelize, so Exclusive
    # (Mutex) over Shared (RWLock). Keeps ElectionLeader a struct: the value is
    # an immutable record, so get returns a safe value-copy.
    @election_leader = Sync::Exclusive(Etcd::ElectionLeader?).new(nil)

    def initialize(@config : Config, @etcd : Etcd)
    end

    # Campaign for leadership and capture the won election as the fencing token
    # in one step, so a node can never campaign without arming the coordinator
    # (which would make every later update_isr raise StaleLeadership). Blocks
    # until this node is elected. Returns the won ElectionLeader.
    def campaign(advertised_uri : String, lease_id : Int) : Etcd::ElectionLeader
      leader = @etcd.election_campaign(leader_key, advertised_uri, lease_id)
      @election_leader.set(leader)
      leader
    end

    def update_isr(synced_node_ids : Set(Int32)) : Nil
      leader = @election_leader.get
      raise Etcd::StaleLeadership.new(leader_key) unless leader && leader.election == leader_key

      ids = synced_node_ids.map(&.to_s(36)).join(",")
      @etcd.put_if_election_leader(isr_key, ids, leader)
    end

    # The current in-sync replica set, or nil if none has been recorded yet (a
    # fresh cluster). Decodes the same format update_isr writes.
    def isr : Set(Int32)?
      @etcd.get(isr_key).try { |raw| parse_isr(raw) }
    end

    # Yield the parsed ISR set on every change to the ISR key (nil when the key
    # is deleted), until the block breaks. Used by the Controller to wait for
    # this node to (re)appear in the ISR before serving as leader.
    def watch_isr(&)
      @etcd.watch(isr_key) do |value|
        yield value.try { |raw| parse_isr(raw) }
      end
    end

    private def parse_isr(raw : String) : Set(Int32)
      # Sized to the id count (commas + 1) to avoid rehashing; split's block
      # form and adding straight to the set skip the intermediate arrays the
      # split + map + to_set chain would allocate.
      set = Set(Int32).new(raw.count(',') + 1)
      raw.split(',') { |id| set << id.to_i(36) }
      set
    end

    private def leader_key : String
      "#{@config.clustering_etcd_prefix}/leader"
    end

    private def isr_key : String
      "#{@config.clustering_etcd_prefix}/isr"
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

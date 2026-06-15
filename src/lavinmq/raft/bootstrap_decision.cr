require "uri"

module LavinMQ::Raft
  # Pure decision for what a node should do at boot, given its own advertised
  # host, the configured seed list, and whether it already has raft peers.
  #
  # This is the swappable seam: today it decides locally (lowest advertised
  # host bootstraps). A future dynamic variant (probe seeds, elect by node_id)
  # can replace `decide` without changing callers, config, or behaviour
  # contract. Invariant: bootstrap ONLY on an unambiguous lowest-match;
  # any ambiguity falls through to Join (never split-brain).
  module BootstrapDecision
    enum Action
      Resume    # already a member; resume from disk
      Bootstrap # form a single-node cluster
      Join      # contact the seeds and join
    end

    def self.decide(advertised_host : String, seed_uris : Array(URI), has_peers : Bool) : Action
      return Action::Resume if has_peers
      return Action::Bootstrap if seed_uris.empty?
      hosts = seed_uris.compact_map { |u| normalize(u.host) }.sort!
      me = normalize(advertised_host)
      # Ambiguous lowest (duplicate-normalized seed hosts) → never bootstrap.
      return Action::Join if hosts.size > 1 && hosts[0] == hosts[1]
      return Action::Bootstrap if me && hosts.first? == me
      Action::Join
    end

    private def self.normalize(host : String?) : String?
      return nil if host.nil? || host.empty?
      host.lstrip('[').rstrip(']').downcase
    end
  end
end

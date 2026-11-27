require "./raft/node"

module LavinMQ
  module Clustering
    # What an operator can do with a cluster that keeps its own membership,
    # which the HTTP API's /api/cluster is served from, see
    # HTTP::ClusterController. A clustering backend that has it returns it
    # from Runner#cluster_admin.
    module ClusterAdmin
      # A leadership transfer that was accepted, to the member with the
      # clustering id `target`, in `term`
      record Transfer, target : Int32, address : String, term : Int64

      # The leader's view of the cluster, nil if it couldn't be told in time
      abstract def status : Raft::Status?

      # Adds the node at `address` as a non-voting learner
      abstract def add_learner(address : String) : Raft::MembershipError?

      # Makes the learner with clustering id `id` a voter
      abstract def promote(id : Int32) : Raft::MembershipError?

      abstract def remove_member(id : Int32) : Raft::MembershipError?

      # Claims a leadership transfer to `target` (a clustering id or address),
      # or to any member that can take over. Returns why not if it can't be
      # done right now. An accepted transfer has to be followed by #step_down.
      abstract def request_transfer(target : String? = nil) : Transfer | String

      # Stops serving and hands leadership over, without waiting for it
      abstract def step_down(target : Transfer) : Nil
    end
  end
end

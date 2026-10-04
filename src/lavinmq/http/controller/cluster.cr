require "../controller"
require "../../clustering/controller"

module LavinMQ
  module HTTP
    # Operator API for a raft cluster: the membership and leadership transfer.
    # The requests are handled by the leader, a follower proxies them to it.
    class ClusterController < Controller
      def initialize(server : LavinMQ::Server, @cluster : Clustering::RaftController?)
        super(server)
      end

      private def register_routes
        get "/api/cluster" do |context, _params|
          refuse_unless_administrator(context, user(context))
          cluster = require_cluster(context)
          status = require_status(context, cluster)
          JSON.build(context.response) do |json|
            write_status(json, status)
          end
          context
        end

        post "/api/cluster/members" do |context, _params|
          refuse_unless_administrator(context, user(context))
          cluster = require_cluster(context)
          body = parse_body(context)
          address = body["address"]?.try(&.as_s?).presence || bad_request(context, "Field 'address' is required")
          change(context, cluster.node.add_learner(address), 201, address)
        end

        post "/api/cluster/members/:member/promote" do |context, params|
          refuse_unless_administrator(context, user(context))
          cluster = require_cluster(context)
          member = params["member"]
          id = require_member(context, cluster, member)
          change(context, cluster.node.promote(id), 200, member)
        end

        delete "/api/cluster/members/:member" do |context, params|
          refuse_unless_administrator(context, user(context))
          cluster = require_cluster(context)
          member = params["member"]
          id = require_member(context, cluster, member)
          change(context, cluster.node.remove_member(id), 204, member)
        end

        post "/api/cluster/transfer-leadership" do |context, _params|
          refuse_unless_administrator(context, user(context))
          cluster = require_cluster(context)
          body = parse_body(context)
          target = body["target"]?.try(&.as_s?).presence
          case plan = cluster.request_transfer(target)
          in Clustering::RaftController::Transfer
            # The leader stops serving, including this connection, so the
            # response has to be complete before that.
            begin
              context.response.status_code = 202
              {target: plan.address, node_id: plan.target.to_s(36), term: plan.term}.to_json(context.response)
              context.response.close
            ensure
              # The transfer is claimed, so it has to proceed even if the
              # client went away, or no later transfer could be requested
              cluster.step_down(plan)
            end
          in String
            halt(context, 409, {error: "conflict", reason: plan})
          end
          context
        end
      end

      private def require_cluster(context) : Clustering::RaftController
        if cluster = @cluster
          return cluster
        end
        if Config.instance.clustering?
          bad_request(context, "Requires the raft clustering backend")
        else
          not_found(context, "Clustering is not enabled")
        end
      end

      private def require_status(context, cluster : Clustering::RaftController) : Clustering::Raft::Status
        status = cluster.node.status
        unless status && status.role.leader?
          halt(context, 409, {error: "conflict", reason: "This node is not the leader"})
        end
        status
      end

      # The clustering id of the member `ref` (a clustering id or raft address)
      # refers to.
      private def require_member(context, cluster : Clustering::RaftController, ref : String) : Int32
        status = require_status(context, cluster)
        status.resolve(ref) || not_found(context, "#{ref} is not a member of the cluster")
      end

      private def change(context, error : Clustering::Raft::MembershipError?, success : Int32, member : String)
        if error
          case error
          in .unknown_member?
            not_found(context, "#{member} is not a member of the cluster")
          in .lost?
            halt(context, 409, {error: "conflict", reason: "#{error.message}, check the cluster status"})
          in .unreachable?
            halt(context, 409, {error: "conflict", reason: "Couldn't reach #{member}, start it first"})
          in .not_leader?, .not_serving?, .pending?, .already_member?, .not_learner?,
             .is_leader?, .not_in_isr?, .not_caught_up?, .address_in_use?
            halt(context, 409, {error: "conflict", reason: error.message})
          end
        end
        context.response.status_code = success
        context
      end

      private def write_status(json : JSON::Builder, status : Clustering::Raft::Status) : Nil
        membership = status.membership
        json.object do
          json.field "leader", status.address
          json.field "term", status.term
          json.field "isr" do
            json.array { status.committed_isr.try &.each { |id| json.string id.to_s(36) } }
          end
          json.field "members" do
            json.array do
              addresses = membership.try(&.addresses) || {status.id => status.address}
              addresses.to_a.sort_by!(&.[1]).each do |id, addr|
                me = id == status.id
                json.object do
                  json.field "address", addr
                  json.field "node_id", id.to_s(36)
                  json.field "role", membership.try(&.learners.includes?(id)) ? "learner" : "voter"
                  json.field "in_isr", status.committed_isr.try(&.includes?(id)) || false
                  json.field "match_index", me ? status.last_index : status.match_index[id]?
                  json.field "caught_up", (me || status.caught_up.includes?(id))
                  json.field "leader", me
                end
              end
            end
          end
        end
      end
    end
  end
end

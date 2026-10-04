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

        post "/api/cluster/members/:address/promote" do |context, params|
          refuse_unless_administrator(context, user(context))
          cluster = require_cluster(context)
          address = params["address"]
          change(context, cluster.node.promote(address), 200, address)
        end

        delete "/api/cluster/members/:address" do |context, params|
          refuse_unless_administrator(context, user(context))
          cluster = require_cluster(context)
          address = params["address"]
          change(context, cluster.node.remove_member(address), 204, address)
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
              {target: plan.target, term: plan.term}.to_json(context.response)
              context.response.close
            ensure
              # The transfer is claimed, so it has to proceed even if the
              # client went away, or no later transfer could be requested
              cluster.step_down(plan.target)
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

      private def change(context, error : Clustering::Raft::MembershipError?, success : Int32, address : String)
        if error
          case error
          in .unknown_member?
            not_found(context, "#{address} is not a member of the cluster")
          in .lost?
            halt(context, 409, {error: "conflict", reason: "#{error.message}, check the cluster status"})
          in .not_leader?, .not_serving?, .pending?, .already_member?, .not_learner?,
             .is_leader?, .not_in_isr?, .not_caught_up?
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
              members = membership.try(&.members) || Set{status.address}
              members.to_a.sort.each do |addr|
                node_id = status.node_id_of(addr)
                json.object do
                  json.field "address", addr
                  json.field "node_id", node_id.try &.to_s(36)
                  json.field "role", membership.try(&.learners.includes?(addr)) ? "learner" : "voter"
                  json.field "in_isr", !node_id.nil? && (status.committed_isr.try(&.includes?(node_id)) || false)
                  json.field "match_index", addr == status.address ? status.last_index : status.match_index[addr]?
                  json.field "caught_up", (addr == status.address || status.caught_up.includes?(addr))
                  json.field "leader", addr == status.address
                end
              end
            end
          end
        end
      end
    end
  end
end

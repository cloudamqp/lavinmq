require "../controller"
require "../../clustering/dr_control"

module LavinMQ
  module HTTP
    # Cross-region DR control: read this region's role and promote/demote it by
    # setting `{prefix}/upstream_etcd`. Served on the management API (admin auth).
    # Promotion during a failover (primary down) is handled locally on every
    # node's internal unix socket by FollowerDRHandler.
    class ClusterController < Controller
      private def register_routes
        get "/api/cluster/dr" do |context, _params|
          refuse_unless_management(context, user(context))
          dr_guarded(context) do
            context.response.content_type = "application/json"
            Clustering::DRControl.status.to_json(context.response)
          end
          context
        end

        put "/api/cluster/dr" do |context, _params|
          refuse_unless_administrator(context, user(context))
          dr_guarded(context) do
            body = parse_body(context)
            value = body["upstream_etcd"]?.try &.as_s?
            bad_request(context, "Field 'upstream_etcd' is required") if value.nil? || value.empty?
            Clustering::DRControl.set(value)
            context.response.status_code = 204
          end
          context
        end

        delete "/api/cluster/dr" do |context, _params|
          refuse_unless_administrator(context, user(context))
          dr_guarded(context) do
            Clustering::DRControl.clear
            context.response.status_code = 204
          end
          context
        end
      end

      # Returns 409 when clustering is disabled and turns a local-etcd outage
      # into a 503 instead of letting Etcd::Error bubble up.
      private def dr_guarded(context, &)
        unless Config.instance.clustering?
          context.response.status_code = 409
          {error: "clustering_disabled", reason: "Clustering is not enabled on this node"}.to_json(context.response)
          return
        end
        yield
      rescue ex : Etcd::Error
        context.response.status_code = 503
        {error: "etcd_unavailable", reason: ex.message}.to_json(context.response)
      end
    end

    # Serves the DR control routes on a follower/relay node's internal unix
    # socket (which otherwise just returns 503), so an operator can promote a DR
    # region during failover even though the management API is proxied to the
    # (down) primary. The socket is local and trusted (mode 0660), so no auth is
    # applied here; non-DR paths fall through to the 503 fallback handler.
    class FollowerDRHandler
      include Router

      def initialize
        register_routes
      end

      private def register_routes
        get "/api/cluster/dr" do |context, _params|
          dr_guarded(context) do
            context.response.content_type = "application/json"
            Clustering::DRControl.status.to_json(context.response)
          end
          context
        end

        put "/api/cluster/dr" do |context, _params|
          dr_guarded(context) do
            value = upstream_from_body(context)
            if value.nil? || value.empty?
              context.response.status_code = 400
              {error: "bad_request", reason: "Field 'upstream_etcd' is required"}.to_json(context.response)
            else
              Clustering::DRControl.set(value)
              context.response.status_code = 204
            end
          end
          context
        end

        delete "/api/cluster/dr" do |context, _params|
          dr_guarded(context) do
            Clustering::DRControl.clear
            context.response.status_code = 204
          end
          context
        end
      end

      private def upstream_from_body(context) : String?
        if body = context.request.body
          JSON.parse(body)["upstream_etcd"]?.try &.as_s?
        end
      rescue JSON::ParseException
        nil
      end

      private def dr_guarded(context, &)
        yield
      rescue ex : Etcd::Error
        context.response.status_code = 503
        {error: "etcd_unavailable", reason: ex.message}.to_json(context.response)
      end
    end
  end
end

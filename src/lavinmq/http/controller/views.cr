require "../controller"
require "../router"

module LavinMQ
  module HTTP
    class ViewsController
      include Router

      def initialize
        static_view "/", view: "overview"
        static_view "/login"
        static_view "/federation"
        static_view "/shovels"
        static_view "/connections"
        static_view "/consumers"
        static_view "/connection"
        static_view "/channels"
        static_view "/channel"
        static_view "/exchanges"
        static_view "/exchange"
        static_view "/vhosts"
        static_view "/vhost"
        static_view "/queues"
        static_view "/queue"
        static_view "/stream"
        static_view "/unacked"
        static_view "/nodes"
        static_view "/logs"
        static_view "/users"
        static_view "/user"
        static_view "/policies"
        static_view "/operator-policies"
      end

      # Maps a clean route (e.g. /queues) to its baked static page
      # (/queues.html). The security headers and login redirect are applied by
      # StaticController (next in chain), so that direct .html requests are
      # protected the same way.
      macro static_view(path, *, view = nil)
        {% view = path[1..] if view.nil? %}
        get {{ path }} do |context, params|
          context.request.path = {{ "/#{view.id}.html" }}
          call_next(context)
          context
        end
      end
    end
  end
end

require "../controller"
require "../router"

module LavinMQ
  module HTTP
    class ViewsController
      include Router

      def initialize
        static_view "/", view: "overview"
        static_view "/login", auth_required: false
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

      macro static_view(path, *, auth_required = true, view = nil)
        {% view = path[1..] if view.nil? %}
        get {{ path }} do |context, params|
          redirect_unless_logged_in! if {{ auth_required }}
          context.response.headers.add("X-Frame-Options", "SAMEORIGIN")
          context.response.headers.add("Referrer-Policy", "same-origin")
          context.response.headers.add("Content-Security-Policy", "default-src 'none'; style-src 'self'; font-src 'self'; img-src 'self'; connect-src 'self'; script-src 'self' 'sha256-7WqJMeRnYJHbEWj9TgKVXAh9Vz/3wErO1WSGhQ2LTf4='")
          # Rewrite path to .html so StaticController (next in chain)
          context.request.path = {{ "/#{view.id}.html" }}
          call_next(context)
          context
        end
      end

      macro redirect_unless_logged_in!
        if !context.request.cookies.has_key?("m")
          context.response.status = ::HTTP::Status::TEMPORARY_REDIRECT
          context.response.headers["Location"] = "login"
          next context
        end
      end
    end
  end
end

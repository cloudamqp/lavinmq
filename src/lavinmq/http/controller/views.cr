require "../controller"
require "../router"
require "../../version"
require "digest/md5"

module LavinMQ
  module HTTP
    class ViewsController
      include Router

      Log = LavinMQ::Log.for "http.views"

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

      macro static_view(path, *, auth_required = true, view = nil, &block)
        {% view = path[1..] if view.nil? %}
        get {{ path }} do |context, params|
          redirect_unless_logged_in! if {{ auth_required }}
          if_non_match = context.request.headers["If-None-Match"]?
          Log.trace { "static_view path={{ path.id }} etag=#{ETag} if-non-match=#{if_non_match}" }
          if if_non_match == ETag
            context.response.status_code = 304
          else
            context.response.content_type = "text/html;charset=utf-8"
            context.response.headers.add("Cache-Control", "no-cache")
            context.response.headers.add("ETag", ETag)
            context.response.headers.add("X-Frame-Options", "SAMEORIGIN")
            context.response.headers.add("Referrer-Policy", "same-origin")
            context.response.headers.add("Content-Security-Policy", "default-src 'none'; style-src 'self'; font-src 'self'; img-src 'self'; connect-src 'self'; script-src 'self' 'sha256-7WqJMeRnYJHbEWj9TgKVXAh9Vz/3wErO1WSGhQ2LTf4='")
            {{ block.body if block }}
            {% if flag?(:release) %}
              context.response.print({{ read_file("static/#{view.id}.html") }})
            {% else %}
              File.open(File.join(PUBLIC_DIR, {{ "#{view.id}.html" }})) do |file|
                file.read_buffering = false
                IO.copy(file, context.response)
              end
            {% end %}
          end
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

      PUBLIC_DIR = "#{__DIR__}/../../../../static"

      # etag won't change in runtime
      {% if flag?(:release) %}
        ETag = %(W/"#{LavinMQ::VERSION}")
      {% else %}
        ETag = %(W/"{{ `date +%s`.strip }}")
      {% end %}
    end
  end
end

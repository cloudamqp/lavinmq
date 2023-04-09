require "router"
require "../../version"
require "html"
require "digest/md5"

module LavinMQ
  module HTTP
    class ViewsController
      include Router

      def initialize
        static_view "/", "overview"
        static_view "/login"
        static_view "/401"
        static_view "/404"
        static_view "/federation"
        static_view "/shovels"
        static_view "/connections"
        static_view "/connection"
        static_view "/channels"
        static_view "/channel"
        static_view "/exchanges"
        static_view "/exchange"
        static_view "/vhosts"
        static_view "/vhost"
        static_view "/queues"
        static_view "/queue"
        static_view "/nodes"
        static_view "/logs"
        static_view "/users"
        static_view "/user"
        static_view "/policies"
        static_view "/operator-policies"
      end

      # Generate a get handler for given path. If no view is specified, path without initial
      # slash will be used as view.
      #
      # Optional block can be given to modify context or set variables before view is rendered.
      #
      # This would render views/json_data.ecr and change content type:
      #
      # ```
      # static_view "/json", "json_data" do
      #   context.response.content_type = "application/json"
      # end
      # ```
      macro static_view(path, view = nil, &block)
        {% view = path[1..] if view.nil? %}
        get {{path}} do |context, params|
          if_non_match = context.request.headers["If-None-Match"]?
          Log.trace { "static_view path={{path.id}} etag=#{ETag} if-non-match=#{if_non_match}" }
          if if_non_match == ETag
            context.response.status_code = 304
          else
            context.response.content_type = "text/html;charset=utf-8"
            context.response.headers.add("Cache-Control", "no-cache")
            context.response.headers.add("ETag", ETag)
            context.response.headers.add("X-Frame-Options", "SAMEORIGIN")
            context.response.headers.add("Referrer-Policy", "same-origin")
            context.response.headers.add("Content-Security-Policy", "default-src 'none'; style-src 'self'; img-src 'self'; connect-src 'self'; script-src 'self' 'sha256-9nCxy0qjWUXfAqDk5MjMKgu+tHDTvI8ZUAmbmYoCEF8='")
            {{block.body if block}}
            render {{view}}
          end
          context
        end
      end

      # etag won't change in runtime
      {% if flag?(:release) %}
        ETag = %(W/"#{LavinMQ::VERSION}")
      {% else %}
        ETag = %(W/"{{ `date +%s`.strip }}")
      {% end %}

      # Render an ecr file from views dir
      macro render(file)
        ECR.embed "views/{{file.id}}.ecr", context.response
      end

      macro active_path?(path)
        context.request.path == "/#{{{path}}}" ||
          context.request.path == "/#{{{path}}}".chomp('s') ||
          (context.request.path == "/" && {{path}} == :".")
      end
    end
  end
end

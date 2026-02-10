require "../controller"
require "../router"
require "../../version"
require "html"
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
      macro static_view(path, *, auth_required = true, view = nil, &block)
        {% view = path[1..] if view.nil? %}
        get {{ path }} do |context, params|
          redirect_unless_logged_in! if {{ auth_required }}
          if_non_match = context.request.headers["If-None-Match"]?
          etag = etag(context.user)
          Log.trace { "static_view path={{ path.id }} etag=#{etag} if-non-match=#{if_non_match}" }
          if if_non_match == etag
            context.response.status_code = 304
          else
            context.response.content_type = "text/html;charset=utf-8"
            context.response.headers.add("Cache-Control", "no-cache")
            context.response.headers.add("ETag", etag)
            context.response.headers.add("X-Frame-Options", "SAMEORIGIN")
            context.response.headers.add("Referrer-Policy", "same-origin")
            context.response.headers.add("Content-Security-Policy", "default-src 'none'; style-src 'self'; font-src 'self'; img-src 'self'; connect-src 'self'; script-src 'self' 'sha256-7WqJMeRnYJHbEWj9TgKVXAh9Vz/3wErO1WSGhQ2LTf4='")
            {{ block.body if block }}
            render {{ view }}
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

      # etag won't change in runtime
      {% if flag?(:release) %}
        ETagBase = LavinMQ::VERSION
      {% else %}
        ETagBase = "{{ `date +%s`.strip }}"
      {% end %}

      def etag(user)
        beginning = "W/\""
        ending = "\""
        tags = 0u8
        size = beginning.size + ETagBase.size + 1 + sizeof(UInt8) + ending.size
        if u = user
          tags = user.tags.sum(&.to_u8)
        end
        ret = String.build(size) do |str|
          str << beginning
          str << ETagBase
          str << ";"
          str << tags
          str << ending
        end
        ret
      end

      # Render an ecr file from views dir
      macro render(file)
        ECR.embed "views/{{ file.id }}.ecr", context.response
      end

      macro active_path?(path)
        context.request.path == "/#{{{ path }}}" ||
          context.request.path == "/#{{{ path }}}".chomp('s') ||
          (context.request.path == "/" && {{ path }} == ".")
      end
    end
  end
end

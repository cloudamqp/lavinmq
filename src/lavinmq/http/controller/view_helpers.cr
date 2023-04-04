require "html"
require "../../version"
require "digest/md5"

module LavinMQ
  module HTTP
    module ViewHelpers
      # Render an ecr file from views dir
      macro render(file)
        ECR.embed "views/{{file.id}}.ecr", context.response
      end

      # Use in view to html escape output
      # `<% escape varaible_name %>` or `<% escape "string" %>`
      macro escape(value)
        HTML.escape({{value}}, context.response)
      end

      macro active_path?(path)
        context.request.path == "/#{{{path}}}" || (context.request.path == "/" && {{path}} == :".")
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
        # etag won't change in runtime, so it's enough to calculate it once
        %etag = Digest::MD5.hexdigest("{{view.id}} #{VERSION}")
        get {{path}} do |context, params|
          if_non_match = context.request.headers["If-None-Match"]?
          Log.trace { "static_view path={{path.id}} etag=#{%etag} if-non-match=#{if_non_match}" }
          if if_non_match == %etag
            context.response.status_code = 304
          else
            context.response.content_type = "text/html;charset=utf-8"
            context.response.headers.add("Cache-Control", "public,max-age=300")
            context.response.headers.add("ETag", %etag)
            context.response.headers.add("X-Frame-Options", "SAMEORIGIN")
            context.response.headers.add("Referrer-Policy", "same-origin")
            {{block.body if block}}
            render {{view}}
          end
          context
        end
      end
    end
  end
end

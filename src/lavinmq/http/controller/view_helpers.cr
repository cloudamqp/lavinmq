require "html"

module LavinMQ
  module HTTP
    module ViewHelpers
      BUILD_TIME = {{ `date +%s` }}

      # Render an ecr file from views dir
      macro render(file)
        ECR.embed "views/{{file.id}}.ecr", context.response
      end

      # Use in view to html escape output
      # `<% escape varaible_name %>` or `<% escape "string" %>`
      macro escape(value)
        HTML.escape({{value}}, context.response)
      end

      # Generate a get handler for given path. If no view is specified, path without initial
      # slash will be used as view.
      #
      # Optional block can be given to modify context or set variables before view is rendered.
      macro static_view(path, view = nil, &block)
        {% view = path[1..] if view.nil? %}
        get {{path}} do |context, params|
          etag = Digest::MD5.hexdigest("{{view.id}} #{BUILD_TIME}")
          context.response.content_type = "text/html;charset=utf-8"
          if context.request.headers["If-None-Match"]? == etag
            context.response.status_code = 304
          else
            context.response.headers.add("Cache-Control", "public,max-age=300")
            context.response.headers.add("ETag", etag)
            {{block.body if block}}
            render {{view}}
          end
          context
        end
      end
    end
  end
end

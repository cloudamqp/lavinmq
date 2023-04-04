require "http/server/handler"
require "digest/md5"

module LavinMQ
  module HTTP
    class StaticController
      include ::HTTP::Handler

      PUBLIC_DIR = "#{__DIR__}/../../../../static"

      def call(context)
        path = context.request.path
        if context.request.method.in?("GET", "HEAD") && !path.starts_with?("/api/")
          path = File.join(path, "index.html") if path.ends_with?('/') || !path.includes?('.')
          serve(context, path) || call_next(context)
        else
          call_next(context)
        end
      end

      {% if flag?(:release) || flag?(:bake_static) %}
        Files = {
          {{ run("./static/bake", PUBLIC_DIR) }}
        }

        private def serve(context, file_path)
          if bytes = Files[file_path]?
            etag = Digest::MD5.hexdigest(bytes)
            if context.request.headers["If-None-Match"]? == etag
              context.response.status_code = 304
            else
              context.response.headers.add("Cache-Control", "public,max-age=300")
              context.response.headers.add("ETag", etag)
              context.response.content_type = mime_type(file_path)
              context.response.content_length = bytes.size
              if context.request.method == "GET" # HEAD requests don't get bodies
                begin
                  context.response.write(bytes)
                rescue ex : IndexError
                  raise IO::Error.new(cause: ex)
                end
              end
            end
            context
          end
        end
      {% else %}
        private def serve(context, file_path)
          File.open(File.join(PUBLIC_DIR, file_path)) do |file|
            etag = file.info.modification_time.to_unix_ms.to_s
            if context.request.headers["If-None-Match"]? == etag
              context.response.status_code = 304
            else
              context.response.headers.add("Cache-Control", "public,max-age=300")
              context.response.headers.add("ETag", etag)
              context.response.content_type = mime_type(file.path)
              context.response.content_length = file.size
              IO.copy(file, context.response) unless context.request.method == "HEAD"
            end
            context
          end
        rescue File::NotFoundError
        end
      {% end %}

      # ameba:disable Metrics/CyclomaticComplexity
      private def mime_type(path)
        case File.extname(path)
        when ".txt"        then "text/plain;charset=utf-8"
        when ".html"       then "text/html;charset=utf-8"
        when ".css"        then "text/css;charset=utf-8"
        when ".js", ".mjs" then "application/javascript;charset=utf-8"
        when ".png"        then "image/png"
        when ".ico"        then "image/x-icon"
        when ".jpg"        then "image/jpeg"
        when ".gif"        then "image/gif"
        when ".svg"        then "image/svg+xml"
        when ".webp"       then "image/webp"
        else                    "application/octet-stream"
        end
      end
    end
  end
end

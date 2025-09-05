require "http/server/handler"
require "compress/zlib"

module LavinMQ
  module HTTP
    class StaticController
      include ::HTTP::Handler

      PUBLIC_DIR = "#{__DIR__}/../../../../static"

      def call(context)
        path = context.request.path
        if context.request.method.in?("GET", "HEAD") && !path.starts_with?("/api/")
          path = "/docs/index.html" if path == "/docs/"
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
          if static_file = Files[file_path]?
            bytes, etag, deflated = static_file
            if context.request.headers["If-None-Match"]? == etag
              context.response.status_code = 304
            else
              context.response.headers.add("Cache-Control", "no-cache")
              context.response.headers.add("ETag", etag)
              context.response.content_type = mime_type(file_path)
              if deflated
                if context.request.headers["Accept-Encoding"]?.try &.includes?("deflate")
                  context.response.headers.add("Content-Encoding", "deflate")
                  context.response.content_length = bytes.size
                  if context.request.method == "GET" # HEAD requests don't get bodies
                    begin
                      context.response.write(bytes)
                    rescue ex : IndexError
                      raise IO::Error.new(cause: ex)
                    end
                  end
                else # client doesn't want deflated bodies
                  if context.request.method == "GET" # HEAD requests don't get bodies
                    io = IO::Memory.new(bytes)
                    Compress::Zlib::Reader.open(io) do |zlib|
                      IO.copy(zlib, context.response)
                    end
                  end
                end
              else # not deflated
                context.response.content_length = bytes.size
                if context.request.method == "GET" # HEAD requests don't get bodies
                  begin
                    context.response.write(bytes)
                  rescue ex : IndexError
                    raise IO::Error.new(cause: ex)
                  end
                end
              end
            end
            context
          end
        end
      {% else %}
        private def serve(context, file_path)
          L.debug "Serve #{file_path} from static?"
          full_path = File.join(PUBLIC_DIR, file_path)
          raise File::NotFoundError.new("file isn't a file", file: full_path) unless File.file?(full_path)
          File.open(full_path) do |file|
            L.debug "Serving static #{file_path}"
            file.read_buffering = false
            etag = %(W/"#{Digest::MD5.hexdigest(file)}")
            if context.request.headers["If-None-Match"]? == etag
              context.response.status_code = 304
            else
              context.response.headers.add("Cache-Control", "no-cache")
              context.response.headers.add("ETag", etag)
              context.response.content_type = mime_type(file.path)
              context.response.content_length = file.size
              if context.request.method == "GET"
                file.rewind
                IO.copy(file, context.response)
              end
            end
            context
          end
        rescue File::NotFoundError
          # To enable faster frontend development, we try to serve a views from static folder.
          # Use `make dev-ui` to compile and watch for changes.
          return if file_path.starts_with?("/views/")
          view = file_path.lstrip("/")
          view = "overview" if view.empty?
          file_path = "/views/#{view}.html"
          serve(context, file_path)
        end
      {% end %}

      # ameba:disable Metrics/CyclomaticComplexity
      private def mime_type(path)
        case File.extname(path)
        when ".txt"        then "text/plain;charset=utf-8"
        when ".html"       then "text/html;charset=utf-8"
        when ".css"        then "text/css;charset=utf-8"
        when ".js", ".mjs" then "application/javascript"
        when ".png"        then "image/png"
        when ".ico"        then "image/x-icon"
        when ".jpg"        then "image/jpeg"
        when ".gif"        then "image/gif"
        when ".svg"        then "image/svg+xml"
        when ".webp"       then "image/webp"
        when ".yaml"       then "application/yaml"
        else                    "application/octet-stream"
        end
      end
    end
  end
end

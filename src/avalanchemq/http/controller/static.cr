require "../controller"
require "router"
module AvalancheMQ
  class StaticController
    include Router

    def initialize(@public_dir : String)
      register_routes
    end

    private def register_routes
      get "/" do |context, _|
        static(context, "index.html")
      end

      get "/queues" do |context, _params|
        static(context, "queues.html")
      end

      get "/table.js" do |context, _|
        static(context, "table.js")
      end
    end

    private def static(context, filename)
      file_path = File.join(@public_dir, filename)
      file_stats = File.stat(file_path)
      etag = file_stats.mtime.epoch_ms.to_s
      context.response.content_type = mime_type(file_path)
      if context.request.headers["If-None-Match"]? == etag
        context.response.status_code = 304
      else
        context.response.headers.add("Cache-Control", "public")
        context.response.headers.add("ETag", etag)
        context.response.content_length = file_stats.size
        File.open(file_path) do |file|
          IO.copy(file, context.response)
        end
      end
      context
    end

    private def mime_type(path)
      case File.extname(path)
      when ".txt"  then "text/plain;charset=utf-8"
      when ".html" then "text/html;charset=utf-8"
      when ".css"  then "text/css;charset=utf-8"
      when ".js"   then "application/javascript;charset=utf-8"
      when ".png"  then "image/png"
      when ".jpg"  then "image/jpeg"
      when ".gif"  then "image/gif"
      else              "application/octet-stream"
      end
    end
  end
end

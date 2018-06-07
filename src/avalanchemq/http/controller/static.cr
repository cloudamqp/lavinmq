require "router"
require "baked_file_system"
require "digest/md5"

module AvalancheMQ
  class StaticController
    class Static
      extend BakedFileSystem
      bake_folder "../../../../static"
    end

    include Router

    def initialize
      register_routes
    end

    private def register_routes
      get "/" do |context, _|
        serve(context, "index.html")
      end

      %w(login connections channels queues exchanges users).each do |r|
        get "/#{r}" do |context, _|
          serve(context, "#{r}.html")
        end
      end

      get "/:filename" do |context, params|
        serve(context, params["filename"])
      end

      get "/js/:filename" do |context, params|
        serve(context, "js/#{params["filename"]}")
      end
    end

    BUILD_TIME = {{ "#{`date +%s`}" }}

    private def serve(context, file_path)
      file = nil
      etag = nil
      {% if flag?(:release) %}
        file = Static.get?(file_path) || raise HTTPServer::NotFoundError.new("#{file_path} not found")
        etag = Digest::MD5.hexdigest(file_path + BUILD_TIME)
      {% else %}
        file, etag = static(context, file(file_path))
      {% end %}
      context.response.content_type = mime_type(file_path)
      if context.request.headers["If-None-Match"]? == etag
        context.response.status_code = 304
      else
        context.response.headers.add("Cache-Control", "public,max-age=300")
        context.response.headers.add("ETag", etag)
        context.response.content_length = file.size
        IO.copy(file, context.response)
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

    private def file(filename)
      public_dir = File.join(__DIR__, "..", "..", "..", "..", "static")
      file_path = File.join(public_dir, "#{filename}.html")
      unless File.exists?(file_path)
        file_path = File.join(public_dir, filename)
        unless File.exists?(file_path)
          raise HTTPServer::NotFoundError.new("#{filename} not found")
        end
      end
      file_path
    end

    private def static(context, file_path)
      file_stats = File.stat(file_path)
      etag = file_stats.mtime.epoch_ms.to_s
      {File.open(file_path), etag}
    end
  end
end

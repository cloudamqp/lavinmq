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

      %w(login connections channels queues exchanges).each do |r|
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
      file = Static.get?(file_path) || raise HTTPServer::NotFoundError.new("#{file_path} not found")
      etag = Digest::MD5.hexdigest(file_path + BUILD_TIME)
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
  end
end

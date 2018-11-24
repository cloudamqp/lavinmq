require "http/server/handler"
require "baked_file_system"
require "digest/md5"

module AvalancheMQ
  module HTTP
    class StaticController
      include ::HTTP::Handler

      PUBLIC_DIR = "#{__DIR__}/../../../../static"

      class Release
        extend BakedFileSystem
        bake_folder "../../../../static"
      end

      def call(context)
        path = context.request.path
        if !%w(GET HEAD).includes?(context.request.method) || path.starts_with?("/api/")
          call_next(context)
          return
        end

        is_dir_path = path.ends_with? "/"
        file_path = URI.unescape(path)
        file_path = "#{file_path}index.html" if is_dir_path

        serve(context, file_path) || call_next(context)
      end

      BUILD_TIME = {{ "#{`date +%s`}" }}

      private def serve(context, file_path)
        file = nil
        etag = nil
        {% if flag?(:release) %}
          file = Release.get?(file_path)
          unless file
            file_path = "#{file_path}.html"
            file = Release.get?(file_path)
          end
          etag = Digest::MD5.hexdigest(file_path + BUILD_TIME) if file
        {% else %}
          file_path = File.join(PUBLIC_DIR, file_path)
          file_path = "#{file_path}.html" unless File.exists?(file_path)
          file, etag = static(context, file_path) if File.exists?(file_path)
        {% end %}
        return nil unless file && etag
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
      ensure
        file.try &.close
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
        when ".svg"  then "image/svg+xml"
        else              "application/octet-stream"
        end
      end

      private def static(context, file_path)
        info = File.info(file_path)
        etag = info.modification_time.to_unix_ms.to_s
        {File.open(file_path), etag}
      end
    end
  end
end

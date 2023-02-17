require "http/server/handler"
require "baked_file_system"
require "./templates"
require "digest/md5"

module LavinMQ
  module HTTP
    class StaticController
      include ::HTTP::Handler
      PUBLIC_DIR = "#{__DIR__}/../../../../static"

      class HtmlTemplates < Templates::Registry
        add_dir PUBLIC_DIR, extension: ".html"
      end

      class Release
        extend BakedFileSystem
        bake_folder "../../../../static"
      end

      def initialize
        @templates = HtmlTemplates.new
      end

      def call(context)
        path = context.request.path
        if !%w(GET HEAD).includes?(context.request.method) || path.starts_with?("/api/")
          call_next(context)
          return
        end

        is_dir_path = path.ends_with? "/"
        file_path = URI.decode_www_form(path)
        file_path = "#{file_path}index.html" if is_dir_path

        serve(context, file_path) || call_next(context)
      end

      BUILD_TIME = {{ "#{`date +%s`}" }}

      private def serve(context, file_path)
        file = nil
        etag = nil
        {% if flag?(:release) || flag?(:bake_static) %}
          file = Release.get?(file_path)
          file = Release.get?("#{file_path}.html") unless file
          file = Release.get?("#{file_path}/index.html") unless file
          etag = Digest::MD5.hexdigest(file.path + BUILD_TIME) if file
        {% else %}
          file_path = File.join(PUBLIC_DIR, file_path)
          file_path = "#{file_path}/index.html" if File.directory?(file_path)
          file_path = "#{file_path}.html" unless File.exists?(file_path)
          file, etag = static(context, file_path) if File.exists?(file_path)
        {% end %}
        return nil unless file && etag
        context.response.content_type = mime_type(file.path)
        if context.request.headers["If-None-Match"]? == etag && cache?(context.request.path)
          context.response.status_code = 304
        else
          if cache?(context.request.path)
            context.response.headers.add("Cache-Control", "public,max-age=300")
            context.response.headers.add("ETag", etag)
          end
          context.response.content_length = file.size

          IO.copy(file, context.response)
        end
        context
      ensure
        file.try &.close
      end

      private def cache?(request_path)
        {% if flag?(:release) %}
          true
        {% else %}
          !request_path.starts_with?("/docs/")
        {% end %}
      end

      private def mime_type(path)
        case File.extname(path)
        when ".txt"         then "text/plain;charset=utf-8"
        when ".html"        then "text/html;charset=utf-8"
        when ".css"         then "text/css;charset=utf-8"
        when ".js", ".mjs"  then "application/javascript;charset=utf-8"
        when ".png", ".ico" then "image/png"
        when ".jpg"         then "image/jpeg"
        when ".gif"         then "image/gif"
        when ".svg"         then "image/svg+xml"
        else                     "application/octet-stream"
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

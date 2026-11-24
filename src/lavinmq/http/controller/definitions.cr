require "uri"
require "../controller"
require "../../definitions"

module LavinMQ
  module HTTP
    class DefinitionsController < Controller
      private def register_routes
        get "/api/definitions" do |context, _params|
          refuse_unless_administrator(context, user(context))
          GlobalDefinitions.new(@server).export(context.response.output)
          context
        end

        post "/api/definitions" do |context, _params|
          refuse_unless_administrator(context, user(context))
          body = parse_body(context)
          GlobalDefinitions.new(@server).import(body)
          context
        end

        post "/api/definitions/upload" do |context, _params|
          refuse_unless_administrator(context, user(context))
          body = json_body?(context.request) ? parse_body(context) : uploaded_file(context)
          GlobalDefinitions.new(@server).import(body)
          redirect_back(context) if context.request.headers["Referer"]?
          context
        end

        get "/api/definitions/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_administrator(context, user(context))
            VHostDefinitions.new(@server, vhost).export(context.response.output)
          end
        end

        post "/api/definitions/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_administrator(context, user(context))
            body = parse_body(context)
            VHostDefinitions.new(@server, vhost).import(body)
          end
        end

        post "/api/definitions/:vhost/upload" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            VHostDefinitions.new(@server, vhost).import(uploaded_file(context))
          end
        end
      end

      # The definitions in the "file" part of a multipart/form-data body
      private def uploaded_file(context) : JSON::Any
        request = context.request
        unless media_type(request) == "multipart/form-data"
          unsupported_content_type(context, request.headers["Content-Type"]?)
        end
        file = nil
        # Parsed after FormData.parse returns: halting inside it can write two error bodies
        ::HTTP::FormData.parse(request) do |part|
          file ||= part.body.gets_to_end if part.name == "file"
        end
        file = file || bad_request(context, "Field 'file' is required")
        parse_json_object(context, IO::Memory.new(file))
      rescue ex : ::HTTP::FormData::Error | MIME::Multipart::Error
        bad_request(context, ex.message.to_s)
      end
    end
  end
end

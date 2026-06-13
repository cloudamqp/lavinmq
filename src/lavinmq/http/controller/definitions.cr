require "uri"
require "../controller"
require "../../definitions"

module LavinMQ
  module HTTP
    class DefinitionsController < Controller
      private def register_routes
        get "/api/definitions" do |context, _params|
          refuse_unless_administrator(context, user(context))
          GlobalDefinitions.new(@amqp_server).export(context.response)
          context
        end

        post "/api/definitions" do |context, _params|
          refuse_unless_administrator(context, user(context))
          body = parse_body(context)
          GlobalDefinitions.new(@amqp_server).import(body)
          context
        end

        post "/api/definitions/upload" do |context, _params|
          refuse_unless_administrator(context, user(context))
          if context.request.headers["Content-Type"] == "application/json"
            body = parse_body(context)
            GlobalDefinitions.new(@amqp_server).import(body)
          else
            ::HTTP::FormData.parse(context.request) do |part|
              if part.name == "file"
                body = JSON.parse(part.body)
                GlobalDefinitions.new(@amqp_server).import(body)
              end
            end
          end
          redirect_back(context) if context.request.headers["Referer"]?
          context
        end

        get "/api/definitions/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_administrator(context, user(context))
            VHostDefinitions.new(@amqp_server, vhost).export(context.response)
          end
        end

        post "/api/definitions/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_administrator(context, user(context))
            body = parse_body(context)
            VHostDefinitions.new(@amqp_server, vhost).import(body)
          end
        end

        post "/api/definitions/:vhost/upload" do |context, params|
          refuse_unless_administrator(context, user(context))
          with_vhost(context, params) do |vhost|
            ::HTTP::FormData.parse(context.request) do |part|
              if part.name == "file"
                body = JSON.parse(part.body)
                VHostDefinitions.new(@amqp_server, vhost).import(body)
              end
            end
          end
        end
      end
    end
  end
end

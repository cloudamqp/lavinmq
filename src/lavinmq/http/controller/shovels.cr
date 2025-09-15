require "../controller.cr"

module LavinMQ
  module HTTP
    class ShovelsController < Controller
      include StatsHelpers

      private def register_routes
        get "/api/shovels" do |context, _params|
          itrs = vhosts(user(context)).flat_map do |v|
            v.shovels.each_value
          end
          page(context, itrs)
        end

        get "/api/shovels/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            page(context, @amqp_server.vhosts[vhost].shovels.each_value)
          end
        end

        get "/api/shovels/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            shovel_name = params["name"]
            if shovel = @amqp_server.vhosts[vhost].shovels[shovel_name]?
              shovel.to_json(context.response)
            else
              context.response.status_code = 404
            end
          end
        end

        put "/api/shovels/:vhost/:name/pause" do |context, params|
          with_vhost(context, params) do |vhost|
            shovel_name = params["name"]
            if current_shovel = @amqp_server.vhosts[vhost].shovels[shovel_name]?
              if !current_shovel.running?
                context.response.status_code = 422
                next
              end
              current_shovel.pause
              context.response.status_code = 204
            else
              context.response.status_code = 404
            end
          end
        end

        put "/api/shovels/:vhost/:name/resume" do |context, params|
          with_vhost(context, params) do |vhost|
            shovel_name = params["name"]
            if current_shovel = @amqp_server.vhosts[vhost].shovels[shovel_name]?
              if !current_shovel.paused?
                context.response.status_code = 422
                next
              end
              current_shovel.resume
              context.response.status_code = 204
            else
              context.response.status_code = 404
            end
          end
        end

        put "/api/shovels/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            shovel_name = params["name"]
            body = parse_body(context)
            
            # Validate required fields
            unless body["src-uri"]? && body["dest-uri"]?
              bad_request(context, "Fields 'src-uri' and 'dest-uri' are required")
            end
            
            is_update = @amqp_server.vhosts[vhost].shovels[shovel_name]?
            begin
              @amqp_server.vhosts[vhost].shovels.create(shovel_name, body)
              context.response.status_code = is_update ? 204 : 201
            rescue ex : JSON::Error
              bad_request(context, ex.message)
            end
          end
        end

        delete "/api/shovels/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            shovel_name = params["name"]
            if @amqp_server.vhosts[vhost].shovels.delete(shovel_name)
              context.response.status_code = 204
            else
              context.response.status_code = 404
            end
          end
        end
      end
    end
  end
end

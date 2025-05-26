require "../controller.cr"
module LavinMQ
  module HTTP
    class  ShovelsController < Controller
      include StatsHelpers

      private def register_routes
        get "/api/shovels" do |context, _params|
          itrs = vhosts(user(context)).flat_map do |v|
            v.shovels.not_nil!.each_value
          end
          page(context, itrs)
        end

        get "/api/shovels/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            page(context, @amqp_server.vhosts[vhost].shovels.not_nil!.each_value)
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
              if ! current_shovel.running?
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

        #TODO 911: found a bug when we restart the LMQ instance - shovel gets back to running
        put "/api/shovels/:vhost/:name/resume" do |context, params|
          with_vhost(context, params) do |vhost|
            shovel_name = params["name"]
            if current_shovel = @amqp_server.vhosts[vhost].shovels[shovel_name]?
              if ! current_shovel.paused?
                context.response.status_code = 422
              end
              current_shovel.resume
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

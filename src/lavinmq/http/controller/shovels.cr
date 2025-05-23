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

        put "/api/shovels/:vhost/:name/pause" do |context, params|
          with_vhost(context, params) do |vhost|
            name = params["name"]
            oldParameter = @amqp_server.vhosts[vhost].parameters[{"shovel", name}]
            oldParameter.value.as_h["state"] = JSON::Any.new("Paused")
            @amqp_server.vhosts[vhost].add_parameter(oldParameter)
            shovels = @amqp_server.vhosts[vhost].shovels[name]
            shovels.to_json(context.response)
          end
        end

        #TODO 911: found a bug when we restart the LMQ instance - shovel gets back to running
        put "/api/shovels/:vhost/:name/resume" do |context, params|
          with_vhost(context, params) do |vhost|
            name = params["name"]
            oldParameter = @amqp_server.vhosts[vhost].parameters[{"shovel", name}]
            oldParameter.value.as_h["state"] = JSON::Any.new("Running")
            @amqp_server.vhosts[vhost].add_parameter(oldParameter)
            shovels = @amqp_server.vhosts[vhost].shovels[name]
            shovels.to_json(context.response)
          end
        end
      end
    end
  end
end

require "../controller"
require "./connections"

module AvalancheMQ
  class ConsumersController < Controller
    include ConnectionsHelper

    private def register_routes
      get "/api/consumers" do |context, _params|
        all_consumers(user(context)).to_json(context.response)
        context
      end

      get "/api/consumers/:vhost" do |context, params|
        with_vhost(context, params) do |vhost|
          user = user(context)
          refuse_unless_management(context, user, vhost)
          c = connections(user).find { |conn| conn.vhost.name == vhost }
          if c
            c.channels.values.flat_map { |ch| ch.consumers }.to_json(context.response)
          else
            context.response.print("[]")
          end
        end
      end
    end

    private def all_consumers(user)
      connections(user).flat_map { |c| c.channels.values.flat_map &.consumers }
    end
  end
end

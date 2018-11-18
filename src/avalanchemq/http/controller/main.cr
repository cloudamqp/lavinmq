require "../controller"
require "../../version"

module AvalancheMQ
  class MainController < Controller
    private def register_routes
      get "/api/overview" do |context, _params|
        x_vhost = context.request.headers["x-vhost"]?
        channels = 0
        connections = 0
        exchanges = 0
        queues = 0
        consumers = 0
        vhosts(user(context)).each do |vhost|
          next unless x_vhost.nil? || vhost.name == x_vhost
          vhost_connections = @amqp_server.connections.select { |c| c.vhost.name == vhost.name }
          connections += vhost_connections.size
          channels += vhost_connections.reduce(0) { |memo, i| memo + i.channels.size }
          consumers += nr_of_consumers(vhost_connections)
          exchanges += vhost.exchanges.size
          queues += vhost.queues.size
        end

        {
          "avalanchemq_version": AvalancheMQ::VERSION,
          "object_totals":       {
            "channels":    channels,
            "connections": connections,
            "consumers":   consumers,
            "exchanges":   exchanges,
            "queues":      queues,
          },
          "listeners":      @amqp_server.listeners,
          "exchange_types": VHost::EXCHANGE_TYPES.map { |name| {"name": name} },
        }.to_json(context.response)
        context
      end

      get "/api/whoami" do |context, _params|
        user(context).user_details.to_json(context.response)
        context
      end

      get "/api/aliveness-test/:vhost" do |context, params|
        with_vhost(context, params) do |vhost|
          @amqp_server.vhosts[vhost].declare_queue("aliveness-test", false, false)
          @amqp_server.vhosts[vhost].bind_queue("aliveness-test", "amq.direct", "aliveness-test")
          msg = Message.new(Time.utc_now.to_unix_ms,
            "amq.direct",
            "aliveness-test",
            AMQP::Properties.new,
            4_u64,
            IO::Memory.new("test"))
          ok = @amqp_server.vhosts[vhost].publish(msg)
          env = @amqp_server.vhosts[vhost].queues["aliveness-test"].get(true)
          ok = ok && env && env.message.body_io.read_string(env.message.size) == "test"
          {status: ok ? "ok" : "failed"}.to_json(context.response)
        end
      end

      get "/api/shovels" do |context, _params|
        shovels = [] of Hash(String, String)
        vhosts(user(context)).each do |vhost|
          vhost.shovels.not_nil!.each do |shovel|
            shovels << map_shovel(shovel)
          end
        end
        shovels.to_json(context.response)
        context
      end

      get "/api/shovels/:vhost" do |context, params|
        shovels = [] of Hash(String, String)
        with_vhost(context, params) do |vhost|
          @amqp_server.vhosts[vhost].shovels.not_nil!.each do |shovel|
            shovels << map_shovel(shovel)
          end
        end
        shovels.to_json(context.response)
        context
      end

      get "/api/federation-links" do |context, _params|
        links = [] of Hash(String, String)
        vhosts(user(context)).each do |vhost|
          vhost.upstreams.not_nil!.each do |upstream|
            upstream.links.each do |resource, publisher|
              links << map_link(upstream, resource, publisher)
            end
          end
        end
        links.to_json(context.response)
        context
      end

      get "/api/federation-links/:vhost" do |context, params|
        links = [] of Hash(String, String)
        with_vhost(context, params) do |vhost|
          @amqp_server.vhosts[vhost].upstreams.not_nil!.each do |upstream|
            upstream.links.each do |resource, publisher|
              links << map_link(upstream, resource, publisher)
            end
          end
        end
        links.to_json(context.response)
        context
      end
    end

    private def nr_of_consumers(connections)
      connections.reduce(0) do |memo_i, i|
        memo_i + i.channels.values.reduce(0) { |memo_j, j| memo_j + j.consumers.size }
      end
    end

    private def map_shovel(shovel)
      {
        "name"  => shovel.name,
        "vhost" => shovel.vhost.name,
        "state" => shovel.state,
      }
    end

    private def map_link(upstream, resource, publisher)
      {
        "name"      => upstream.name,
        "vhost"     => upstream.vhost.name,
        "timestamp" => publisher.connected_at.to_s,
        "type"      => upstream.is_a?(QueueUpstream) ? "queue" : "exchange",
        "uri"       => upstream.uri.to_s,
        "resource"  => resource,
      }
    end
  end
end

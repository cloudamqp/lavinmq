require "base64"
require "uri"
require "./queues"
require "./exchanges"
require "../binding_helpers"
require "../controller"

module LavinMQ
  module HTTP
    class BindingsController < Controller
      include BindingHelpers
      include QueueHelpers
      include ExchangeHelpers

      # ameba:disable Metrics/CyclomaticComplexity
      private def register_routes
        get "/api/bindings" do |context, _params|
          itr = Iterator(BindingDetails)
            .chain(vhosts(user(context)).map { |v| bindings(v) })
          page(context, itr)
        end

        get "/api/bindings/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            page(context, bindings(@amqp_server.vhosts[vhost]))
          end
        end

        get "/api/bindings/:vhost/e/:name/q/:queue" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            e = exchange(context, params, vhost)
            q = queue(context, params, vhost, "queue")
            itr = e.queue_bindings.each.select { |(_, v)| v.includes?(q) }
              .map { |(k, _)| e.binding_details(k, q) }
            if e.name.empty?
              default_binding = BindingDetails.new("", q.vhost.name, {q.name, nil}, q)
              itr = {default_binding}.each.chain(itr)
            end
            page(context, itr)
          end
        end

        post "/api/bindings/:vhost/e/:name/q/:queue" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            e = exchange(context, params, vhost)
            q = queue(context, params, vhost, "queue")
            user = user(context)
            if !user.can_read?(vhost, e.name)
              access_refused(context, "User doesn't have read permissions to exchange '#{e.name}'")
            elsif !user.can_write?(vhost, q.name)
              access_refused(context, "User doesn't have write permissions to queue '#{q.name}'")
            elsif e.name.empty?
              access_refused(context, "Not allowed to bind to the default exchange")
            end
            body = parse_body(context)
            routing_key = body["routing_key"]?.try(&.as_s?) ||
                          body["routingKey"]?.try(&.as_s?)
            arguments = (args = body["arguments"]?.try(&.as_h?)) ? AMQP::Table.new(args) : AMQP::Table.new
            unless routing_key
              bad_request(context, "Field 'routing_key' is required")
            end
            ok = e.vhost.bind_queue(q.name, e.name, routing_key, arguments)
            props = BindingDetails.hash_key({routing_key, arguments})
            context.response.headers["Location"] = q.name + "/" + props
            context.response.status_code = 201
            Log.debug do
              binding = binding_for_props(context, e, q, props)
              "exchange '#{e.name}' bound to queue '#{q.name}' with key '#{binding}' #{ok}"
            end
          end
        end

        get "/api/bindings/:vhost/e/:name/q/:queue/:props" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            e = exchange(context, params, vhost)
            q = queue(context, params, vhost, "queue")
            props = URI.decode_www_form(params["props"])
            binding = binding_for_props(context, e, q, props)
            e.binding_details(binding[0], q).to_json(context.response)
          end
        end

        delete "/api/bindings/:vhost/e/:name/q/:queue/*props" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            e = exchange(context, params, vhost)
            q = queue(context, params, vhost, "queue")
            user = user(context)
            if !user.can_read?(vhost, e.name)
              access_refused(context, "User doesn't have read permissions to exchange '#{e.name}'")
            elsif !user.can_write?(vhost, q.name)
              access_refused(context, "User doesn't have write permissions to queue '#{q.name}'")
            elsif e.name.empty?
              access_refused(context, "Not allowed to unbind from the default exchange")
            end
            props = URI.decode_www_form(params["props"])
            found = false
            e.queue_bindings.each do |k, destinations|
              next unless destinations.includes?(q) && BindingDetails.hash_key(k) == props
              arguments = k[1] || AMQP::Table.new
              @amqp_server.vhosts[vhost].unbind_queue(q.name, e.name, k[0], arguments)
              found = true
              Log.debug { "exchange '#{e.name}' unbound from queue '#{q.name}' with key '#{k}'" }
              break
            end
            context.response.status_code = found ? 204 : 404
          end
        end

        get "/api/bindings/:vhost/e/:name/e/:destination" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            source = exchange(context, params, vhost)
            destination = exchange(context, params, vhost, "destination")
            page(context, source.exchange_bindings.each.select { |(_, v)| v.includes?(destination) }
              .map { |(k, _)| source.binding_details(k, destination) })
          end
        end

        post "/api/bindings/:vhost/e/:name/e/:destination" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            source = exchange(context, params, vhost)
            destination = exchange(context, params, vhost, "destination")
            user = user(context)
            if !user.can_read?(vhost, source.name)
              access_refused(context, "User doesn't have read permissions to exchange '#{source.name}'")
            elsif !user.can_write?(vhost, destination.name)
              access_refused(context, "User doesn't have write permissions to exchange '#{destination.name}'")
            elsif source.name.empty? || destination.name.empty?
              access_refused(context, "Not allowed to bind to the default exchange")
            elsif destination.internal?
              bad_request(context, "Not allowed to bind to an internal exchange")
            end
            body = parse_body(context)
            routing_key = body["routing_key"]?.try(&.as_s?) ||
                          body["routingKey"]?.try(&.as_s?)
            arguments = (args = body["arguments"]?.try(&.as_h?)) ? AMQP::Table.new(args) : AMQP::Table.new
            unless routing_key
              bad_request(context, "Field 'routing_key' is required")
            end
            source.vhost.bind_exchange(destination.name, source.name, routing_key, arguments)
            props = BindingDetails.hash_key({routing_key, arguments})
            context.response.headers["Location"] = context.request.path + "/" + props
            context.response.status_code = 201
          end
        end

        get "/api/bindings/:vhost/e/:name/e/:destination/:props" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            source = exchange(context, params, vhost)
            destination = exchange(context, params, vhost, "destination")
            props = URI.decode_www_form(params["props"])
            binding = binding_for_props(context, source, destination, props)
            source.binding_details(binding[0], destination).to_json(context.response)
          end
        end

        delete "/api/bindings/:vhost/e/:name/e/:destination/*props" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            source = exchange(context, params, vhost)
            destination = exchange(context, params, vhost, "destination")
            user = user(context)
            if !user.can_read?(vhost, source.name)
              access_refused(context, "User doesn't have read permissions to exchange '#{source.name}'")
            elsif !user.can_write?(vhost, destination.name)
              access_refused(context, "User doesn't have write permissions to queue '#{destination.name}'")
            elsif source.name.empty? || destination.name.empty?
              access_refused(context, "Not allowed to unbind from the default exchange")
            elsif destination.internal?
              bad_request(context, "Not allowed to unbind from an internal exchange")
            end
            props = URI.decode_www_form(params["props"])
            found = false
            source.exchange_bindings.each do |k, destinations|
              next unless destinations.includes?(destination) && BindingDetails.hash_key(k) == props
              arguments = k[1] || AMQP::Table.new
              @amqp_server.vhosts[vhost].unbind_exchange(destination.name, source.name, k[0], arguments)
              found = true
              break
            end
            context.response.status_code = found ? 204 : 404
          end
        end
      end
    end
  end
end

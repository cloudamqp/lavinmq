require "base64"
require "uri"
require "./queues"
require "./exchanges"
require "../controller"
require "../resource_helper"

module AvalancheMQ
  module BindingHelpers
    private def bindings(vhost)
      vhost.exchanges.values.flat_map do |e|
        e.bindings_details.map { |b| map_binding(b) }
      end
    end

    private def map_binding(b)
      key_tuple = {b[:routing_key].as(String), b[:arguments].as(Hash(String, AMQP::Field))}
      b.merge({properties_key: hash_key(key_tuple)})
    end

    private def binding_for_props(context, source, destination, props)
      binding = source.bindings.select do |k, v|
        v.includes?(destination) && hash_key(k) == props
      end.first?
      unless binding
        type = destination.is_a?(Queue) ? "queue" : "exchange"
        not_found(context, "Binding '#{props}' on exchange '#{source.name}' -> #{type} '#{destination.name}' does not exist")
      end
      binding
    end

    private def hash_key(key : Tuple(String, Hash(String, AMQP::Field)))
      hsh = Base64.urlsafe_encode(key[1].to_s)
      "#{key[0]}~#{hsh}"
    end

    private def unbind_prop(source : Queue | Exchange, destination : Queue | Exchange, key : String)
      k = source.bindings.keys.find { |k| hash_key(k) == key }
      source.unbind(destination, k[0], k[1]) if k
    end
  end

  class BindingsController < Controller
    include ResourceHelper
    include BindingHelpers
    include QueueHelpers
    include ExchangeHelpers

    private def register_routes
      get "/api/bindings" do |context, _params|
        vhosts(user(context)).flat_map { |v| bindings(v) }.to_json(context.response)
        context
      end

      get "/api/bindings/:vhost" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          bindings(@amqp_server.vhosts[vhost]).to_json(context.response)
        end
      end

      get "/api/bindings/:vhost/e/:name/q/:queue" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          e = exchange(context, params, vhost)
          q = queue(context, params, vhost, "queue")
          e.bindings.select { |_k, v| v.includes?(q) }
            .map { |k, _| map_binding(e.binding_details(k, q)) }
            .to_json(context.response)
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
          end
          body = parse_body(context)
          routing_key = body["routing_key"].as_s?
          arguments = parse_arguments(body)
          unless routing_key && arguments
            bad_request(context, "Fields 'routing_key' and 'arguments' are required")
          end
          e.vhost.bind_queue(q.name, e.name, routing_key, arguments)
          props = hash_key({routing_key, arguments})
          context.response.headers["Location"] = context.request.path + "/" + props
          context.response.status_code = 201
        end
      end

      get "/api/bindings/:vhost/e/:name/q/:queue/:props" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          e = exchange(context, params, vhost)
          q = queue(context, params, vhost, "queue")
          props = URI.unescape(params["props"])
          binding = binding_for_props(context, e, q, props)
          map_binding(e.binding_details(binding[0], q)).to_json(context.response)
        end
      end

      delete "/api/bindings/:vhost/e/:name/q/:queue/:props" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          e = exchange(context, params, vhost)
          q = queue(context, params, vhost, "queue")
          user = user(context)
          if !user.can_read?(vhost, e.name)
            access_refused(context, "User doesn't have read permissions to exchange '#{e.name}'")
          elsif !user.can_write?(vhost, q.name)
            access_refused(context, "User doesn't have write permissions to queue '#{q.name}'")
          end
          props = URI.unescape(params["props"])
          unbind_prop(e, q, props)
          context.response.status_code = 204
        end
      end

      get "/api/bindings/:vhost/e/:name/e/:destination" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          source = exchange(context, params, vhost)
          destination = exchange(context, params, vhost, "destination")
          source.bindings.select { |_k, v| v.includes?(destination) }
            .map { |k, _| map_binding(source.binding_details(k, destination)) }
            .to_json(context.response)
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
          end
          body = parse_body(context)
          routing_key = body["routing_key"].as_s?
          arguments = parse_arguments(body)
          unless routing_key && arguments
            bad_request(context, "Fields 'routing_key' and 'arguments' are required")
          end
          source.vhost.bind_exchange(destination.name, source.name, routing_key, arguments)
          props = hash_key({routing_key, arguments})
          context.response.headers["Location"] = context.request.path + "/" + props
          context.response.status_code = 201
        end
      end

      get "/api/bindings/:vhost/e/:name/e/:destination/:props" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          source = exchange(context, params, vhost)
          destination = exchange(context, params, vhost, "destination")
          props = URI.unescape(params["props"])
          binding = binding_for_props(context, source, destination, props)
          map_binding(source.binding_details(binding[0], destination)).to_json(context.response)
        end
      end

      delete "/api/bindings/:vhost/e/:name/e/:destination/:props" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          source = exchange(context, params, vhost)
          destination = exchange(context, params, vhost, "destination")
          user = user(context)
          if !user.can_read?(vhost, source.name)
            access_refused(context, "User doesn't have read permissions to exchange '#{source.name}'")
          elsif !user.can_write?(vhost, destination.name)
            access_refused(context, "User doesn't have write permissions to queue '#{destination.name}'")
          end
          props = URI.unescape(params["props"])
          unbind_prop(source, destination, props)
          context.response.status_code = 204
        end
      end
    end
  end
end

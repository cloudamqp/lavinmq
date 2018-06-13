require "uri"
require "./queues"
require "./exchanges"
require "../controller"
require "../resource_helper"

module AvalancheMQ
  module BindingHelpers
    private def bindings(vhost)
      vhost.exchanges.values.flat_map do |e|
        e.bindings_details
      end
    end

    private def binding_for_props(context, source, destination, props)
      binding = source.bindings.select do |k, v|
        v.includes?(destination) && Exchange.hash_key(k) == props
      end.first?
      unless binding
        type = destination.is_a?(Queue) ? "queue" : "exchange"
        not_found(context, "Binding '#{props}' on exchange '#{source.name}' -> #{type} '#{destination.name}' does not exist")
      end
      binding
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
          e.bindings.select { |k, v| v.includes?(q) }
                    .map { |k, _| e.binding_details(k, q) }
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
          key = e.vhost.bind_queue(q.name, e.name, routing_key, arguments)
          props = Exchange.hash_key(key.not_nil!)
          context.response.headers["Location"] = context.request.path + "/" + props
          context.response.status_code = 201
        end
      end

      get "/api/bindings/:vhost/e/:name/q/:queue/:props" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          e = exchange(context, params, vhost)
          q = queue(context, params, vhost, "queue")
          props = params["props"]
          binding = binding_for_props(context, e, q, props)
          e.binding_details(binding[0], q).to_json(context.response)
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
          props = params["props"]
          e.unbind_prop(q, props)
          context.response.status_code = 204
        end
      end

      get "/api/bindings/:vhost/e/:name/e/:destination" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          source = exchange(context, params, vhost)
          destination = exchange(context, params, vhost, "destination")
          source.bindings.select { |k, v| v.includes?(destination) }
                         .map { |k, _| source.binding_details(k, destination) }
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
          key = source.vhost.bind_exchange(destination.name, source.name, routing_key, arguments)
          props = Exchange.hash_key(key.not_nil!)
          context.response.headers["Location"] = context.request.path + "/" + props
          context.response.status_code = 201
        end
      end

      get "/api/bindings/:vhost/e/:name/e/:destination/:props" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          source = exchange(context, params, vhost)
          destination = exchange(context, params, vhost, "destination")
          props = params["props"]
          binding = binding_for_props(context, source, destination, props)
          source.binding_details(binding[0], destination).to_json(context.response)
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
          props = params["props"]
          source.unbind_prop(destination, props)
          context.response.status_code = 204
        end
      end
    end
  end
end

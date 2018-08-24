require "uri"
require "../controller"
require "../resource_helper"

module AvalancheMQ
  module QueueHelpers
    private def queue(context, params, vhost, key = "name")
      name = URI.unescape(params[key])
      q = @amqp_server.vhosts[vhost].queues[name]?
      not_found(context, "Queue #{name} does not exist") unless q
      q
    end
  end

  class QueuesController < Controller
    include ResourceHelper
    include QueueHelpers

    private def register_routes
      get "/api/queues" do |context, _params|
        vhosts(user(context)).flat_map { |v| v.queues.values }.to_json(context.response)
        context
      end

      get "/api/queues/:vhost" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          @amqp_server.vhosts[vhost].queues.values.to_json(context.response)
        end
      end

      get "/api/queues/:vhost/:name" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          name = params["name"]
          q = @amqp_server.vhosts[vhost].queues[name]?
          not_found(context, "Queue #{name} does not exist") unless q
          q.details.merge({
            consumer_details: q.consumers.to_a,
          }).to_json(context.response)
        end
      end

      put "/api/queues/:vhost/:name" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          user = user(context)
          name = params["name"]
          name = Queue.generate_name if name.empty?
          body = parse_body(context)
          durable = body["durable"]?.try(&.as_bool?) || false
          auto_delete = body["auto_delete"]?.try(&.as_bool?) || false
          arguments = parse_arguments(body)
          dlx = arguments["x-dead-letter-exchange"]?.try &.as?(String)
          dlx_ok = dlx.nil? || (user.can_write?(vhost, dlx) && user.can_read?(vhost, name))
          unless user.can_config?(vhost, name) && dlx_ok
            access_refused(context, "User doesn't have permissions to declare queue '#{name}'")
          end
          q = @amqp_server.vhosts[vhost].queues[name]?
          if q
            unless q.match?(durable, auto_delete, arguments)
              bad_request(context, "Existing queue declared with other arguments arg")
            end
            context.response.status_code = 200
          elsif name.starts_with? "amq."
            bad_request(context, "Not allowed to use the amq. prefix")
          else
            @amqp_server.vhosts[vhost]
                        .declare_queue(name, durable, auto_delete, arguments)
            context.response.status_code = 204
          end
        end
      end

      delete "/api/queues/:vhost/:name" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          q = queue(context, params, vhost)
          user = user(context)
          unless user.can_config?(q.vhost.name, q.name)
            access_refused(context, "User doesn't have permissions to delete queue '#{q.name}'")
          end
          if context.request.query_params["if-unused"]? == "true"
            bad_request(context, "Exchange #{q.name} in vhost #{q.vhost.name} in use") if q.in_use?
          end
          q.delete
          context.response.status_code = 204
        end
      end

      get "/api/queues/:vhost/:name/bindings" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_management(context, user(context), vhost)
          queue = queue(context, params, vhost)
          all_bindings = queue.vhost.exchanges.values.flat_map(&.bindings_details)
          all_bindings.select { |b| b[:destination] == queue.name }.to_json(context.response)
        end
      end

      delete "/api/queues/:vhost/:name/contents" do |context, params|
        with_vhost(context, params) do |vhost|
          user = user(context)
          refuse_unless_management(context, user, vhost)
          q = queue(context, params, vhost)
          unless user.can_read?(vhost, q.name)
            access_refused(context, "User doesn't have permissions to read queue '#{q.name}'")
          end
          q.purge
          context.response.status_code = 204
        end
      end

      post "/api/queues/:vhost/:name/get" do |context, params|
        with_vhost(context, params) do |vhost|
          user = user(context)
          refuse_unless_management(context, user, vhost)
          q = queue(context, params, vhost)
          unless user.can_read?(q.vhost.name, q.name)
            access_refused(context, "User doesn't have permissions to read queue '#{q.name}'")
          end
          body = parse_body(context)
          count = body["count"]?.try(&.as_i)
          ack_mode = body["ack_mode"]?.try(&.as_s)
          encoding = body["encoding"]?.try(&.as_s)
          truncate = body["truncate"]?.try(&.as_i)
          unless count && ack_mode && encoding
            bad_request(context, "Fields 'count', 'ack_mode' and 'encoding' are required")
          end
          case ack_mode
          when "ack_requeue_true", "reject_requeue_true", "peek"
            msgs = q.peek(count)
            redelivered = true
          when "ack_requeue_false", "reject_requeue_false", "get"
            msgs = Array.new(count) { q.get(true) }
            redelivered = false
          else
            bad_request(context, "Unknown encoding #{encoding}")
          end
          msgs ||= [] of Envelope
          count = q.message_count
          res = msgs.compact.map do |env|
            payload = String.new(truncate.nil? || env.message.size <= truncate ? env.message.body : env.message.body[0, truncate])
            case encoding
            when "auto"
              if payload.valid_encoding?
                content = payload
                payload_encoding = "string"
              else
                content = Base64.urlsafe_encode(payload)
                payload_encoding = "base64"
              end
            when "base64"
              content = Base64.urlsafe_encode(payload)
              payload_encoding = "base64"
            else
              bad_request(context, "Unknown encoding #{encoding}")
            end
            {
              "payload_bytes":    env.message.size,
              "redelivered":      redelivered,
              "exchange":         env.message.exchange_name,
              "routing_key":      env.message.routing_key,
              "message_count":    count,
              "properties":       env.message.properties,
              "payload":          content,
              "payload_encoding": payload_encoding,
              "peek":             ack_mode == "peek",
            }
          end
          res.to_json(context.response)
        end
      end
    end
  end
end

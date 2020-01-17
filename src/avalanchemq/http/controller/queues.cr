require "uri"
require "../controller"
require "../resource_helpers"
require "../binding_helpers"

module AvalancheMQ
  module HTTP
    module QueueHelpers
      private def queue(context, params, vhost, key = "name")
        name = URI.decode_www_form(params[key])
        q = @amqp_server.vhosts[vhost].queues[name]?
        not_found(context, "Queue #{name} does not exist") unless q
        q
      end
    end

    class QueuesController < Controller
      include ResourceHelpers
      include BindingHelpers
      include QueueHelpers

      # ameba:disable Metrics/CyclomaticComplexity
      private def register_routes
        get "/api/queues" do |context, _|
          itr = Iterator(Queue).chain(vhosts(user(context)).map { |v| v.queues.each_value })
          page(context, itr)
        end

        get "/api/queues/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            page(context, @amqp_server.vhosts[vhost].queues.each_value)
          end
        end

        get "/api/queues/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            q = queue(context, params, vhost)
            q.details_tuple.merge({
              consumer_details: q.consumers.to_a,
            }).to_json(context.response)
          end
        end

        put "/api/queues/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            user = user(context)
            name = URI.decode_www_form(params["name"])
            name = Queue.generate_name if name.empty?
            body = parse_body(context)
            durable = body["durable"]?.try(&.as_bool?) || false
            auto_delete = body["auto_delete"]?.try(&.as_bool?) || false
            arguments = parse_arguments(body)
            tbl = AMQP::Table.new arguments
            dlx = arguments["x-dead-letter-exchange"]?.try &.as?(String)
            dlx_ok = dlx.nil? || (user.can_write?(vhost, dlx) && user.can_read?(vhost, name))
            unless user.can_config?(vhost, name) && dlx_ok
              access_refused(context, "User doesn't have permissions to declare queue '#{name}'")
            end
            q = @amqp_server.vhosts[vhost].queues[name]?
            if q
              unless q.match?(durable, auto_delete, tbl)
                bad_request(context, "Existing queue declared with other arguments arg")
              end
              context.response.status_code = 200
            elsif name.starts_with? "amq."
              bad_request(context, "Not allowed to use the amq. prefix")
            else
              @amqp_server.vhosts[vhost]
                .declare_queue(name, durable, auto_delete, tbl)
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
            @amqp_server.vhosts[vhost].delete_queue(q.name)
            context.response.status_code = 204
          end
        end

        get "/api/queues/:vhost/:name/bindings" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            queue = queue(context, params, vhost)
            itr = bindings(queue.vhost).select { |b| b.destination.name == queue.name }
            page(context, itr)
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
            get_count = body["count"]?.try(&.as_i) || 1
            ack_mode = body["ack_mode"]?.try(&.as_s) || "ack_requeue_true"
            encoding = body["encoding"]?.try(&.as_s) || "auto"
            truncate = body["truncate"]?.try(&.as_i)
            requeue = body["requeue"]?.try(&.as_bool) || ack_mode.ends_with?("requeue_true")
            ack = ack_mode.starts_with?(/(ack_|get)/)
            JSON.build(context.response) do |j|
              j.array do
                get_count.times do
                  sp = SegmentPosition.new 0, 0
                  q.basic_get(false) do |env|
                    break if env.nil?
                    sp = env.segment_position
                    size = truncate.nil? ? env.message.size : Math.min(truncate, env.message.size)
                    payload = String.build(size) do |io|
                      IO.copy env.message.body_io, io, size
                    end
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
                    j.object do
                      j.field("payload_bytes", env.message.size)
                      j.field("redelivered", env.redelivered)
                      j.field("exchange", env.message.exchange_name)
                      j.field("routing_key", env.message.routing_key)
                      j.field("message_count", q.message_count)
                      j.field("properties", env.message.properties)
                      j.field("payload", content)
                      j.field("payload_encoding", payload_encoding)
                    end
                  end
                  if ack
                    q.ack(sp, true)
                    q.publish(sp) if requeue
                  else
                    q.reject(sp, requeue)
                  end
                end
              end
            end
          end
        end
      end
    end
  end
end

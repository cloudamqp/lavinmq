require "uri"
require "../controller"
require "../binding_helpers"

module LavinMQ
  module HTTP
    module QueueHelpers
      private def queue(context, params, vhost, key = "name")
        name = URI.decode_www_form(params[key])
        q = @amqp_server.vhosts[vhost].queues[name]?
        not_found(context, "Not Found") unless q
        q
      end
    end

    class QueuesController < Controller
      include BindingHelpers
      include QueueHelpers

      # ameba:disable Metrics/CyclomaticComplexity
      private def register_routes
        get "/api/queues" do |context, _|
          itr = Iterator(Queue).chain(vhosts(user(context)).map &.queues.each_value)
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
            consumer_limit = context.request.query_params["consumer_list_length"]?.try &.to_i || -1
            JSON.build(context.response) do |json|
              queue(context, params, vhost).to_json(json, consumer_limit)
            end
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
            tbl = (args = body["arguments"]?.try(&.as_h?)) ? AMQP::Table.new(args) : AMQP::Table.new
            dlx = tbl["x-dead-letter-exchange"]?.try &.as?(String)
            dlx_ok = dlx.nil? || (user.can_write?(vhost, dlx) && user.can_read?(vhost, name))
            unless user.can_config?(vhost, name) && dlx_ok
              access_refused(context, "User doesn't have permissions to declare queue '#{name}'")
            end
            q = @amqp_server.vhosts[vhost].queues[name]?
            if q
              unless q.match?(durable, false, auto_delete, tbl)
                bad_request(context, "Existing queue declared with other arguments arg")
              end
              context.response.status_code = 204
            elsif name.starts_with? "amq."
              bad_request(context, "Not allowed to use the amq. prefix")
            else
              begin
                @amqp_server.vhosts[vhost]
                  .declare_queue(name, durable, auto_delete, tbl)
                context.response.status_code = 201
              rescue e : LavinMQ::Error::PreconditionFailed
                bad_request(context, e.message)
              end
            end
          end
        end

        put "/api/queues/:vhost/:name/pause" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            q = queue(context, params, vhost)
            q.pause!
            context.response.status_code = 204
          end
        end

        put "/api/queues/:vhost/:name/resume" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            q = queue(context, params, vhost)
            q.resume!
            context.response.status_code = 204
          end
        end

        get "/api/queues/:vhost/:name/size-details" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            JSON.build(context.response) do |json|
              queue(context, params, vhost).size_details_to_json(json)
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
              bad_request(context, "Queue #{q.name} in vhost #{q.vhost.name} in use") if q.in_use?
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
            default_binding = BindingDetails.new("", queue.vhost.name, {queue.name, nil}, queue)
            page(context, {default_binding}.each.chain(itr))
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
            count = context.request.query_params["count"]? || ""
            if count.empty?
              q.purge
            else
              count_i = count.to_i?
              bad_request(context, "Count must be a number") if count_i.nil?
              bad_request(context, "Count must be greater than 0") if count_i <= 0
              q.purge(count_i)
            end
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
            if q.state != QueueState::Running && q.state != QueueState::Paused
              forbidden(context, "Can't get from queue that is not in running state")
            end
            body = parse_body(context)
            get_count = body["count"]?.try(&.as_i) || 1
            ack_mode = (body["ack_mode"]? || body["ackmode"]?).try(&.as_s) || "get"
            encoding = body["encoding"]?.try(&.as_s) || "auto"
            truncate = body["truncate"]?.try(&.as_i)
            requeue = body["requeue"]?.try(&.as_bool) || ack_mode == "reject_requeue_true"
            ack = ack_mode == "get"
            bad_request(context, "Cannot requeue message on get") if ack && requeue
            JSON.build(context.response) do |j|
              j.array do
                sps = Array(SegmentPosition).new(get_count)
                get_count.times do
                  q.basic_get(false, true) do |env|
                    sps << env.segment_position
                    size = truncate ? Math.min(truncate, env.message.bodysize) : env.message.bodysize
                    payload = String.new(env.message.body[0, size])
                    case encoding
                    when "base64"
                      content = Base64.urlsafe_encode(payload)
                      payload_encoding = "base64"
                    else
                      if payload.valid_encoding?
                        content = payload
                        payload_encoding = "string"
                      else
                        content = Base64.urlsafe_encode(payload)
                        payload_encoding = "base64"
                      end
                    end
                    j.object do
                      j.field("payload_bytes", env.message.bodysize)
                      j.field("redelivered", env.redelivered)
                      j.field("exchange", env.message.exchange_name)
                      j.field("routing_key", env.message.routing_key)
                      j.field("message_count", q.message_count)
                      j.field("properties", env.message.properties)
                      j.field("payload", content)
                      j.field("payload_encoding", payload_encoding)
                    end
                  end || break
                end
                sps.each do |sp|
                  if ack
                    q.ack(sp)
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

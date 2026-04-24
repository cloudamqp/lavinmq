require "../controller"
require "../../name_validator"

module LavinMQ
  module HTTP
    class SimpleHttpController < Controller
      private def register_routes
        post "/api/simple/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            user = user(context)
            name = params["name"]
            unless user.can_write?(vhost.name, name)
              access_refused(context, "User doesn't have permissions to write to queue '#{name}'")
            end
            q = vhost.queues[name]? || ensure_classic_queue(context, vhost, user, name)
            if q.is_a?(LavinMQ::AMQP::Stream)
              halt(context, 409, {error:  "conflict",
                                  reason: "Queue '#{name}' is not supported by this endpoint"})
            end
            size, body_io = read_body(context, vhost)
            msg = Message.new(RoughTime.unix_ms, "", name,
              AMQ::Protocol::Properties.new, size, body_io)
            vhost.publish(msg)
            vhost.event_tick(EventType::ClientPublish)
            context.response.status_code = 201
          end
        end

        post "/api/simple/:vhost/:name/get" do |context, params|
          with_vhost(context, params) do |vhost|
            user = user(context)
            name = params["name"]
            unless user.can_read?(vhost.name, name)
              access_refused(context, "User doesn't have permissions to read from queue '#{name}'")
            end
            q = vhost.queues[name]?
            not_found(context, "Queue '#{name}' not found") if q.nil?
            if q.is_a?(LavinMQ::AMQP::Stream)
              halt(context, 409, {error:  "conflict",
                                  reason: "Queue '#{name}' is not supported by this endpoint"})
            end
            delivered = q.basic_get(no_ack: true) do |env|
              body = env.message.body
              context.response.content_type = "application/octet-stream"
              context.response.status_code = 200
              context.response.content_length = body.bytesize
              context.response.write(body)
              vhost.event_tick(EventType::ClientGetNoAck)
              vhost.add_send_bytes(env.message.bodysize)
            end
            context.response.status_code = 204 unless delivered
          end
        end
      end

      private def ensure_classic_queue(context, vhost, user, name)
        unless user.can_config?(vhost.name, name)
          access_refused(context, "User doesn't have permissions to declare queue '#{name}'")
        end
        if NameValidator.reserved_prefix?(name)
          bad_request(context, "Prefix #{NameValidator::PREFIX_LIST} forbidden, please choose another name")
        end
        if name.bytesize > UInt8::MAX
          bad_request(context, "Queue name too long, can't exceed 255 characters")
        end
        vhost.declare_queue(name, durable: true, auto_delete: false)
        vhost.queues[name]
      end

      # Buffers the request body into IO::Memory. Streaming straight from
      # context.request.body is unsafe because Exchange#publish calls
      # `body_io.seek` to rewind between deliveries, which HTTP request
      # bodies don't support.
      private def read_body(context, vhost) : {UInt64, IO::Memory}
        max_size = Config.instance.max_message_size
        buf = IO::Memory.new
        if body = context.request.body
          copied = IO.copy(body, buf, max_size + 1).to_u64
          vhost.add_recv_bytes(copied)
          halt_too_large(context, max_size) if copied > max_size
          buf.rewind
          {copied, buf}
        else
          {0_u64, buf}
        end
      end

      private def halt_too_large(context, max_size)
        halt(context, 413, {error:  "payload_too_large",
                            reason: "Max message size is #{max_size} bytes"})
      end
    end
  end
end

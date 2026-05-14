require "uri"
require "base64"
require "../controller"
require "../../replay/stamp"

module LavinMQ
  module HTTP
    class ReplayController < Controller
      private def register_routes
        get "/api/replay/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            list = vhost.queues.select(LavinMQ::AMQP::ReplayQueue).map do |q|
              overview(q.as(LavinMQ::AMQP::ReplayQueue), vhost)
            end
            list.to_json(context.response)
          end
        end

        get "/api/replay/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            q = find_replay_queue(context, params, vhost)
            items = [] of NamedTuple(id: String, source: String?, exchange: String?, routing_key: String?,
              rule_id: String?, timestamp: Int64?, payload_bytes: UInt64, content_type: String?,
              delivery_count: Int64?)
            q.each_envelope do |env|
              items << build_item(env)
            end
            items.to_json(context.response)
          end
        end

        get "/api/replay/:vhost/:name/:id" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            q = find_replay_queue(context, params, vhost)
            env = find_envelope_or_404(context, q, params["id"])
            full_item(env).to_json(context.response)
          end
        end

        delete "/api/replay/:vhost/:name/:id" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context), vhost)
            q = find_replay_queue(context, params, vhost)
            env = find_envelope_or_404(context, q, params["id"])
            q.delete_envelope(env.segment_position)
            context.response.status_code = 204
          end
        end
      end

      private def overview(q, vhost)
        {
          name:     q.name,
          vhost:    vhost.name,
          messages: q.message_count,
          durable:  q.durable?,
        }
      end

      private def find_replay_queue(context, params, vhost) : LavinMQ::AMQP::ReplayQueue
        name = params["name"]
        q = vhost.queue?(name)
        not_found(context, "Replay queue '#{name}' not found") unless q
        unless q.is_a?(LavinMQ::AMQP::ReplayQueue)
          not_found(context, "Queue '#{name}' is not a replay queue (x-queue-type: replay)")
        end
        q
      end

      private def find_envelope_or_404(context, q, id : String)
        env = q.find_envelope_with_header(LavinMQ::Replay::HEADER_REPLAY_ID, id)
        not_found(context, "No replay message with id '#{id}'") unless env
        env
      end

      private def header_string(headers, key)
        headers.try { |h| h[key]?.try(&.to_s) }
      end

      private def header_int(headers, key) : Int64?
        v = headers.try(&.[key]?)
        return nil if v.nil?
        v.as?(Int).try(&.to_i64)
      end

      private def build_item(env)
        msg = env.message
        h = msg.properties.headers
        {
          id:             header_string(h, LavinMQ::Replay::HEADER_REPLAY_ID) || "",
          source:         header_string(h, LavinMQ::Replay::HEADER_SOURCE_QUEUE),
          exchange:       header_string(h, LavinMQ::Replay::HEADER_SOURCE_EXCHANGE),
          routing_key:    header_string(h, LavinMQ::Replay::HEADER_SOURCE_ROUTING_KEY),
          rule_id:        header_string(h, LavinMQ::Replay::HEADER_SOURCE_RULE_ID),
          timestamp:      header_int(h, LavinMQ::Replay::HEADER_SOURCE_TIMESTAMP),
          payload_bytes:  msg.bodysize,
          content_type:   msg.properties.content_type,
          delivery_count: header_int(h, "x-delivery-count"),
        }
      end

      private def full_item(env)
        msg = env.message
        headers = msg.properties.headers
        payload, encoding = encode_body(msg)
        {
          id:               header_string(headers, LavinMQ::Replay::HEADER_REPLAY_ID) || "",
          source:           header_string(headers, LavinMQ::Replay::HEADER_SOURCE_QUEUE),
          exchange:         header_string(headers, LavinMQ::Replay::HEADER_SOURCE_EXCHANGE),
          routing_key:      header_string(headers, LavinMQ::Replay::HEADER_SOURCE_ROUTING_KEY),
          rule_id:          header_string(headers, LavinMQ::Replay::HEADER_SOURCE_RULE_ID),
          timestamp:        header_int(headers, LavinMQ::Replay::HEADER_SOURCE_TIMESTAMP),
          delivery_count:   header_int(headers, "x-delivery-count"),
          payload:          payload,
          payload_encoding: encoding,
          payload_bytes:    msg.bodysize,
          content_type:     msg.properties.content_type,
          properties:       properties_hash(msg.properties),
        }
      end

      private def encode_body(msg : LavinMQ::BytesMessage) : Tuple(String, String)
        payload = msg.body
        if Unicode.valid?(payload)
          {String.new(payload), "string"}
        else
          {Base64.urlsafe_encode(payload), "base64"}
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def properties_hash(p)
        out = {} of String => JSON::Any
        if v = p.content_type
          out["content_type"] = JSON::Any.new(v)
        end
        if v = p.content_encoding
          out["content_encoding"] = JSON::Any.new(v)
        end
        if v = p.delivery_mode
          out["delivery_mode"] = JSON::Any.new(v.to_i64)
        end
        if v = p.priority
          out["priority"] = JSON::Any.new(v.to_i64)
        end
        if v = p.correlation_id
          out["correlation_id"] = JSON::Any.new(v)
        end
        if v = p.reply_to
          out["reply_to"] = JSON::Any.new(v)
        end
        if v = p.expiration
          out["expiration"] = JSON::Any.new(v)
        end
        if v = p.message_id
          out["message_id"] = JSON::Any.new(v)
        end
        if v = p.type
          out["type"] = JSON::Any.new(v)
        end
        if v = p.user_id
          out["user_id"] = JSON::Any.new(v)
        end
        if v = p.app_id
          out["app_id"] = JSON::Any.new(v)
        end
        if h = p.headers
          headers_out = {} of String => JSON::Any
          h.each do |hk, hv|
            headers_out[hk] = JSON::Any.new(hv.to_s)
          end
          out["headers"] = JSON::Any.new(headers_out)
        end
        out
      end
    end
  end
end

require "uri"
require "base64"
require "../controller"
require "../../replay/stamp"
require "../../rough_time"

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

        patch "/api/replay/:vhost/:name/:id" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context), vhost)
            q = find_replay_queue(context, params, vhost)
            env = find_envelope_or_404(context, q, params["id"])
            body = parse_body(context)
            force = context.request.query_params["force"]? == "true"
            patch_envelope(context, q, env, body, force)
            context.response.status_code = 204
          end
        end

        post "/api/replay/:vhost/:name/:id/release" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context), vhost)
            q = find_replay_queue(context, params, vhost)
            env = find_envelope_or_404(context, q, params["id"])
            reset = context.request.query_params["reset_replay"]? == "true"
            release_envelope(context, q, env, vhost, reset)
            context.response.status_code = 204
          end
        end
      end

      EDITABLE_CONTENT_TYPES = {
        "text/",
        "application/json",
        "application/xml",
        "application/x-www-form-urlencoded",
      }

      private def editable_content_type?(content_type : String?) : Bool
        return false unless content_type
        EDITABLE_CONTENT_TYPES.any? do |allowed|
          allowed.ends_with?("/") ? content_type.starts_with?(allowed) : content_type == allowed
        end
      end

      private def patch_envelope(context, q, env, body : JSON::Any, force : Bool)
        msg = env.message
        new_body_str = body["body"]?.try(&.as_s?)
        new_headers = body["headers"]?.try(&.as_h?)
        unless force || new_body_str.nil? || editable_content_type?(msg.properties.content_type)
          halt(context, 415, {error:  "unsupported_content_type",
                              reason: "content_type '#{msg.properties.content_type}' is not editable; pass ?force=true to override"})
        end

        body_bytes = new_body_str ? new_body_str.to_slice : msg.body
        new_props = build_patched_properties(msg.properties, new_headers)
        new_msg = LavinMQ::Message.new(
          msg.timestamp,
          msg.exchange_name,
          msg.routing_key,
          new_props,
          body_bytes.bytesize.to_u64,
          IO::Memory.new(body_bytes),
        )
        q.publish(new_msg)
        q.delete_envelope(env.segment_position)
      end

      # Builds a Properties whose:
      # * x-source-* / x-source-timestamp / x-source-rule-id are
      #   carried over unchanged so origin metadata survives the edit.
      # * x-replay-id is dropped so the intake stamp generates a fresh
      #   id (clients addressing the message by id won't keep working
      #   against the stale id).
      # * Other user-authored headers are replaced by `new_headers`
      #   when provided; otherwise carried over verbatim.
      # ameba:disable Metrics/CyclomaticComplexity
      private def build_patched_properties(p, new_headers)
        result_headers = AMQ::Protocol::Table.new
        if existing = p.headers
          existing.each do |k, v|
            next if k == LavinMQ::Replay::HEADER_REPLAY_ID
            result_headers[k] = v if k.starts_with?("x-source-")
          end
        end
        if new_headers
          new_headers.each do |k, v|
            next if k == LavinMQ::Replay::HEADER_REPLAY_ID
            next if k.starts_with?("x-source-")
            result_headers[k] = v.as_s? || v.to_s
          end
        elsif existing = p.headers
          existing.each do |k, v|
            next if k == LavinMQ::Replay::HEADER_REPLAY_ID
            next if k.starts_with?("x-source-")
            result_headers[k] = v
          end
        end
        AMQ::Protocol::Properties.new(
          content_type: p.content_type,
          content_encoding: p.content_encoding,
          headers: result_headers,
          delivery_mode: p.delivery_mode,
          priority: p.priority,
          correlation_id: p.correlation_id,
          reply_to: p.reply_to,
          expiration: p.expiration,
          message_id: p.message_id,
          timestamp: p.timestamp_raw,
          type: p.type,
          user_id: p.user_id,
          app_id: p.app_id,
          reserved1: p.reserved1,
        )
      end

      private def release_envelope(context, q, env, vhost, reset_replay : Bool)
        msg = env.message
        headers = msg.properties.headers
        source_exchange = header_string(headers, LavinMQ::Replay::HEADER_SOURCE_EXCHANGE)
        source_rk = header_string(headers, LavinMQ::Replay::HEADER_SOURCE_ROUTING_KEY)
        if source_exchange.nil? || source_rk.nil?
          bad_request(context, "Replay message missing x-source-exchange/x-source-routing-key; cannot release")
        end
        new_headers = AMQ::Protocol::Table.new
        if headers
          headers.each do |k, v|
            next if k == LavinMQ::Replay::HEADER_REPLAY_ID
            next if k == LavinMQ::Replay::HEADER_SOURCE_TIMESTAMP
            next if reset_replay && k.starts_with?("x-source-")
            new_headers[k] = v
          end
        end
        new_props = AMQ::Protocol::Properties.new(
          content_type: msg.properties.content_type,
          content_encoding: msg.properties.content_encoding,
          headers: new_headers,
          delivery_mode: msg.properties.delivery_mode,
          priority: msg.properties.priority,
          correlation_id: msg.properties.correlation_id,
          reply_to: msg.properties.reply_to,
          expiration: msg.properties.expiration,
          message_id: msg.properties.message_id,
          timestamp: msg.properties.timestamp_raw,
          type: msg.properties.type,
          user_id: msg.properties.user_id,
          app_id: msg.properties.app_id,
          reserved1: msg.properties.reserved1,
        )
        ex = source_exchange.as(String)
        rk = source_rk.as(String)
        new_msg = LavinMQ::Message.new(
          RoughTime.unix_ms,
          ex,
          rk,
          new_props,
          msg.bodysize,
          IO::Memory.new(msg.body)
        )
        vhost.publish(new_msg)
        q.delete_envelope(env.segment_position)
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

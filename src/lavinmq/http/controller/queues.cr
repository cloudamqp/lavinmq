require "uri"
require "../controller"
require "../binding_helpers"
require "../../unacked_message"
require "../../name_validator"

module LavinMQ
  module HTTP
    module QueueHelpers
      private def find_queue(context, params, vhost, key = "name")
        name = params[key]
        q = vhost.queue?(name)
        not_found(context, "Not Found") unless q
        q
      end

      private def find_stream(context, vhost, name)
        q = vhost.queue?(name)
        not_found(context, "Not Found") unless q
        not_found(context, "Not Found") unless q.is_a?(LavinMQ::AMQP::Stream)
        q.as(LavinMQ::AMQP::Stream)
      end
    end

    class QueuesController < Controller
      include BindingHelpers
      include QueueHelpers

      # ameba:disable Metrics/CyclomaticComplexity
      private def register_routes
        get "/api/queues" do |context, _|
          itr = vhosts(user(context)).flat_map &.queues
          page(context, itr)
        end

        get "/api/queues/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            page(context, vhost.queues)
          end
        end

        get "/api/queues/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            consumer_limit = context.request.query_params["consumer_list_length"]?.try &.to_i || -1
            JSON.build(context.response) do |json|
              find_queue(context, params, vhost).to_json(json, consumer_limit)
            end
          end
        end

        get "/api/queues/:vhost/:name/unacked" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            q = find_queue(context, params, vhost)
            unacked_messages = q.unacked_messages
            page(context, unacked_messages)
          end
        end

        get "/api/queues/:vhost/:name/processed" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            q = find_queue(context, params, vhost)
            if q.is_a?(LavinMQ::AMQP::Stream)
              bad_request(context, "Stream queues do not record processed messages")
            end
            unless q.is_a?(LavinMQ::AMQP::Queue)
              bad_request(context, "Processed log not available for this queue type")
            end
            from_ts, to_ts = parse_processed_range(context)
            outcome = parse_processed_outcome(context)
            header_match = parse_processed_header_filters(context)
            offset = (context.request.query_params["offset"]?.try &.to_i?) || 0
            limit = (context.request.query_params["limit"]?.try &.to_i?) || 100
            limit = 1000 if limit > 1000
            limit = 1 if limit < 1
            offset = 0 if offset < 0
            records = q.processed_query(from_ts, to_ts, offset, limit, outcome, header_match)
            JSON.build(context.response) do |json|
              json.array do
                records.each { |r| processed_record_to_json(r, json) }
              end
            end
          end
        end

        get "/api/queues/:vhost/:name/processed/summary" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            q = find_queue(context, params, vhost)
            if q.is_a?(LavinMQ::AMQP::Stream)
              bad_request(context, "Stream queues do not record processed messages")
            end
            unless q.is_a?(LavinMQ::AMQP::Queue)
              bad_request(context, "Processed log not available for this queue type")
            end
            from_ts, to_ts = parse_processed_range(context)
            outcome = parse_processed_outcome(context)
            header_match = parse_processed_header_filters(context)
            summary = q.processed_summary(from_ts, to_ts, outcome, header_match)
            JSON.build(context.response) do |json|
              if summary
                processed_summary_to_json(summary, from_ts, to_ts, json)
              else
                json.object { }
              end
            end
          end
        end

        put "/api/queues/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            user = user(context)
            name = params["name"]
            name = AMQP::Queue.generate_name if name.empty?
            body = parse_body(context)
            durable = body["durable"]?.try(&.as_bool?) || false
            auto_delete = body["auto_delete"]?.try(&.as_bool?) || false
            tbl = (args = body["arguments"]?.try(&.as_h?)) ? AMQP::Table.new(args) : AMQP::Table.new
            dlx = tbl["x-dead-letter-exchange"]?.try &.as?(String)
            dlx_ok = dlx.nil? || (user.can_write?(vhost.name, dlx) && user.can_read?(vhost.name, name))
            unless user.can_config?(vhost.name, name) && dlx_ok
              access_refused(context, "User doesn't have permissions to declare queue '#{name}'")
            end
            q = vhost.queue?(name)
            if q
              unless q.match?(durable, false, auto_delete, tbl)
                bad_request(context, "Existing queue declared with other arguments arg")
              end
              context.response.status_code = 204
            elsif NameValidator.reserved_prefix?(name)
              bad_request(context, "Prefix #{NameValidator::PREFIX_LIST} forbidden, please choose another name")
            elsif name.bytesize > UInt8::MAX
              bad_request(context, "Queue name too long, can't exceed 255 characters")
            else
              vhost.declare_queue(name, durable, auto_delete, tbl)
              context.response.status_code = 201
            end
          end
        end

        put "/api/queues/:vhost/:name/pause" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            q = find_queue(context, params, vhost)
            q.pause!
            context.response.status_code = 204
          end
        end

        put "/api/queues/:vhost/:name/resume" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            q = find_queue(context, params, vhost)
            q.resume!
            context.response.status_code = 204
          end
        end

        put "/api/queues/:vhost/:name/restart" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            q = find_queue(context, params, vhost)
            if q.restart!
              context.response.status_code = 204
            else
              bad_request(context, "Queue was not restarted")
            end
          end
        end

        delete "/api/queues/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            q = find_queue(context, params, vhost)
            user = user(context)
            unless user.can_config?(q.vhost.name, q.name)
              access_refused(context, "User doesn't have permissions to delete queue '#{q.name}'")
            end
            if context.request.query_params["if-unused"]? == "true"
              bad_request(context, "Queue #{q.name} in vhost #{q.vhost.name} in use") if q.in_use?
            end
            vhost.delete_queue(q.name)
            context.response.status_code = 204
          end
        end

        get "/api/queues/:vhost/:name/bindings" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            queue = find_queue(context, params, vhost)
            page(context, queue.bindings)
          end
        end

        delete "/api/queues/:vhost/:name/contents" do |context, params|
          with_vhost(context, params) do |vhost|
            user = user(context)
            refuse_unless_management(context, user, vhost)
            q = find_queue(context, params, vhost)
            unless user.can_read?(vhost.name, q.name)
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
            q = find_queue(context, params, vhost)
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
                    # Track vhost-level metrics for HTTP API consumption
                    event_type = ack ? EventType::ClientGet : EventType::ClientGetNoAck
                    vhost.event_tick(event_type)
                    vhost.add_send_bytes(env.message.bodysize.to_u64)
                    j.object do
                      payload_encoding = "string"
                      j.field("payload_bytes", env.message.bodysize)
                      j.field("redelivered", env.redelivered)
                      j.field("exchange", env.message.exchange_name)
                      j.field("routing_key", env.message.routing_key)
                      j.field("message_count", q.message_count)
                      j.field("properties", env.message.properties)
                      j.field("payload") do
                        j.string do |io|
                          payload_encoding = encode_body(env.message, truncate, encoding, io)
                        end
                      end
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
              rescue e : Exception
                # Requeue all unacked messages on error
                if unacked_sps = sps
                  unacked_sps.each do |sp|
                    q.reject(sp, true)
                  end
                end
                raise e
              end
            end
          end
        end

        post "/api/queues/:vhost/:name/stream" do |context, params|
          with_vhost(context, params) do |vhost|
            user = user(context)
            refuse_unless_management(context, user, vhost)
            q = find_stream(context, vhost, params["name"])
            unless user.can_read?(q.vhost.name, q.name)
              access_refused(context, "User doesn't have permissions to read stream '#{q.name}'")
            end
            if q.state != QueueState::Running && q.state != QueueState::Paused
              forbidden(context, "Can't read from stream that is not in running state")
            end
            body = parse_body(context)
            count = body["count"]?.try(&.as_i) || 1
            count = 0 if count < 0
            offset = body["offset"]?.try(&.as_s) || "first"
            offset = offset.to_i64 if /^\d+$/.match(offset)
            encoding = body["encoding"]?.try(&.as_s) || "auto"
            truncate = body["truncate"]?.try(&.as_i)
            reader = q.reader(offset)
            JSON.build(context.response) do |j|
              j.array do
                reader.each do |env|
                  break if count.zero?
                  payload_encoding = "string"
                  j.object do
                    j.field("payload_bytes", env.message.bodysize)
                    j.field("redelivered", env.redelivered)
                    j.field("exchange", env.message.exchange_name)
                    j.field("routing_key", env.message.routing_key)
                    j.field("message_count", q.message_count)
                    j.field("properties", env.message.properties)
                    j.field("payload") do
                      j.string do |io|
                        payload_encoding = encode_body(env.message, truncate, encoding, io)
                      end
                    end
                    j.field("payload_encoding", payload_encoding)
                  end
                  count -= 1
                end
              end
            end
          rescue e : LavinMQ::AMQP::StreamMessageStore::OffsetError
            bad_request(context, e.message)
          end
        end
      end

      private def encode_body(message, truncate, encoding, io) : String
        size = truncate ? Math.min(truncate, message.bodysize) : message.bodysize
        payload = message.body[0, size]
        if encoding == "base64" || !Unicode.valid?(payload)
          Base64.urlsafe_encode(payload, io)
          "base64"
        else
          io.write payload
          "string"
        end
      end

      private def parse_processed_range(context) : Tuple(Int64, Int64)
        params = context.request.query_params
        now = RoughTime.unix_ms
        to_ts = (params["to"]?.try &.to_i64?) || now
        default_from = to_ts - 3_600_000_i64 # 1h window
        from_ts = (params["from"]?.try &.to_i64?) || default_from
        {from_ts, to_ts}
      end

      private def parse_processed_outcome(context) : LavinMQ::ProcessedLog::Outcome?
        raw = context.request.query_params["outcome"]?
        return nil if raw.nil? || raw.empty?
        LavinMQ::ProcessedLog::Outcome.parse?(raw)
      end

      private def parse_processed_header_filters(context) : Hash(String, String)
        result = {} of String => String
        context.request.query_params.each do |key, value|
          if key.starts_with?("header.")
            header_key = key.lchop("header.")
            result[header_key] = value unless header_key.empty?
          end
        end
        result
      end

      private def processed_record_to_json(rec : LavinMQ::ProcessedLog::Record, json : JSON::Builder) : Nil
        json.object do
          json.field "ack_ts", rec.ack_ts_ms
          json.field "latency_ms", rec.latency_ms
          json.field "payload_size", rec.payload_size
          json.field "redelivery_count", rec.redelivery_count
          json.field "outcome", rec.outcome.to_s.downcase
          json.field "exchange", rec.exchange
          json.field "routing_key", rec.routing_key
          json.field "consumer_tag", rec.consumer_tag
          json.field "headers" do
            if h = rec.headers
              h.to_json(json)
            else
              json.object { }
            end
          end
        end
      end

      private def processed_summary_to_json(s : LavinMQ::ProcessedLog::Summary, from_ts : Int64, to_ts : Int64, json : JSON::Builder) : Nil
        json.object do
          json.field "from", from_ts
          json.field "to", to_ts
          json.field "count", s.count
          json.field "outcomes" do
            json.object do
              s.outcomes.each { |name, count| json.field name, count }
            end
          end
          json.field "latency" do
            json.object do
              json.field "p50", s.latency_p50
              json.field "p95", s.latency_p95
              json.field "p99", s.latency_p99
              json.field "avg", s.latency_avg
            end
          end
          json.field "redeliveries" do
            json.object do
              json.field "avg", s.redeliveries_avg
              json.field "max", s.redeliveries_max
              json.field "histogram" do
                json.array do
                  json.number(s.redeliveries_histogram[0])
                  json.number(s.redeliveries_histogram[1])
                  json.number(s.redeliveries_histogram[2])
                  json.number(s.redeliveries_histogram[3])
                end
              end
              json.field "buckets" do
                json.array do
                  json.string "0"
                  json.string "1-3"
                  json.string "4-7"
                  json.string "8+"
                end
              end
            end
          end
          json.field "payload_size_avg", s.payload_size_avg
          json.field "dropped", s.dropped
        end
      end
    end
  end
end

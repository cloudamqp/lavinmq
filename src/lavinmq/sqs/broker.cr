require "uuid"
require "digest/sha256"
require "./errors"
require "./queue_meta"
require "./queue_meta_store"
require "./message_attributes"
require "./message_mapping"
require "./inflight"
require "./metrics"
require "./ids"
require "../vhost"
require "../logger"

module LavinMQ
  module SQS
    # Per-vhost façade between the SQS actions and the AMQP infrastructure.
    # Owns queue metadata, in-flight tracking and the send/receive mapping so
    # the action layer stays protocol-only.
    class Broker
      Log              = LavinMQ::Log.for "sqs.broker"
      DELAYED_EXCHANGE = "sqs.delayed"
      PURGE_INTERVAL   = 60.seconds
      DEDUP_CACHE_TTL  = 5 * 60 * 1000 # ms, SQS deduplication interval

      record SendResult, message_id : String, md5_of_body : String,
        md5_of_attributes : String?, md5_of_system_attributes : String?

      struct ReceivedMessage
        getter receipt_handle : String
        getter message : MessageMapping::Received
        getter receive_count : UInt32
        getter first_receive_ts : Int64

        def initialize(@receipt_handle, @message, @receive_count, @first_receive_ts)
        end

        def md5_of_body : String
          @message.md5_of_body || Checksums.md5_hex(@message.body)
        end
      end

      getter vhost : VHost
      getter metas : QueueMetaStore

      # Per-vhost counters for the `sqs_metrics` detailed Prometheus family;
      # each is also added to the process-wide `Metrics`
      @messages_sent = Atomic(UInt64).new(0_u64)
      @messages_received = Atomic(UInt64).new(0_u64)
      @messages_deleted = Atomic(UInt64).new(0_u64)
      @visibility_timeouts = Atomic(UInt64).new(0_u64)

      {% for name in %w[messages_sent messages_received messages_deleted visibility_timeouts] %}
        def {{ name.id }} : UInt64
          @{{ name.id }}.get(:relaxed)
        end

        private def add_{{ name.id }}(count : Int = 1) : Nil
          @{{ name.id }}.add(count.to_u64, :relaxed)
          Metrics.add_{{ name.id }}(count)
        end
      {% end %}

      def initialize(@vhost : VHost)
        @metas = QueueMetaStore.new(@vhost.data_dir, @vhost.replicator, @vhost.name)
        @inflights = Hash(String, Inflight).new
        @purged_at = Hash(String, Time::Instant).new
        @lock = Mutex.new
        @log = Logger.new(Log, vhost: @vhost.name)
      end

      # Queues internal to the broker (delayed exchange queues) are never SQS queues
      def queue?(name : String) : AMQP::Queue?
        q = @vhost.queue?(name) || return
        return if q.internal?
        q
      end

      def queue(name : String) : AMQP::Queue
        queue?(name) || raise QueueDoesNotExist.new
      end

      # Stored metadata, or defaults for a queue created over AMQP
      def meta(name : String) : QueueMeta
        @metas[name]? || QueueMeta.new(name)
      end

      def create_queue(name : String, attributes : Hash(String, String), tags : Hash(String, String)) : QueueMeta
        if queue?(name)
          if existing = @metas[name]?
            effective = existing.effective_attributes
            attributes.each do |k, v|
              # An unset boolean (e.g. FifoQueue on a standard queue) is false
              current = effective[k]? || ("false" if QueueAttributes::BOOLEANS.includes?(k))
              raise QueueNameExists.new unless current == v
            end
            return existing
          end
          # Adopt a queue that was declared over AMQP
          meta = QueueMeta.new(name, attributes, tags)
          @metas.set(meta)
          return meta
        end
        if @vhost.queue_limit_reached?
          raise OverLimit.new("The queue limit of the vhost (#{@vhost.max_queues}) is reached.")
        end
        meta = QueueMeta.new(name, attributes, tags)
        args = AMQP::Table.new
        args["x-message-ttl"] = meta.message_retention_period.to_i64 * 1000
        if meta.fifo?
          args["x-message-deduplication"] = true
          args["x-deduplication-header"] = MessageMapping::DEDUP_ID_HEADER
          args["x-cache-ttl"] = DEDUP_CACHE_TTL
        end
        @vhost.declare_queue(name, true, false, args)
        ensure_delayed_binding(name) if meta.delay_seconds > 0
        @metas.set(meta)
        meta
      end

      def delete_queue(name : String) : Nil
        queue(name)
        @vhost.delete_queue(name)
        @metas.delete(name)
        @lock.synchronize do
          @inflights.delete(name).try &.close
          @purged_at.delete(name)
        end
      end

      def set_attributes(name : String, attributes : Hash(String, String)) : QueueMeta
        queue(name)
        meta = @metas[name]? || QueueMeta.new(name)
        meta.attributes.merge!(attributes)
        meta.touch
        ensure_delayed_binding(name) if meta.delay_seconds > 0
        @metas.set(meta)
        meta
      end

      def set_tags(name : String, tags : Hash(String, String)) : Nil
        queue(name)
        meta = @metas[name]? || QueueMeta.new(name)
        meta.tags.merge!(tags)
        @metas.set(meta)
      end

      def remove_tags(name : String, keys : Array(String)) : Nil
        queue(name)
        meta = @metas[name]? || return
        keys.each { |k| meta.tags.delete(k) }
        @metas.set(meta)
      end

      def queue_names(prefix : String? = nil) : Array(String)
        names = Array(String).new
        @vhost.each_queue do |q|
          next unless q.is_a?(AMQP::Queue)
          next if q.internal?
          next if prefix && !q.name.starts_with?(prefix)
          names << q.name
        end
        names.sort!
      end

      def send(q : AMQP::Queue, meta : QueueMeta, body : String, attrs : Array(MessageAttribute),
               system_attrs : Array(MessageAttribute), delay_seconds : Int32?,
               group_id : String?, dedup_id : String?, sender_id : String) : SendResult
        if meta.fifo? && dedup_id.nil?
          if meta.content_based_deduplication?
            dedup_id = Digest::SHA256.hexdigest(body)
          else
            raise InvalidParameterValue.new("The queue should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly")
          end
        end
        message_id = Ids.uuid
        md5_of_body = Checksums.md5_hex(body)
        delay = delay_seconds || meta.delay_seconds
        now = RoughTime.unix_ms
        props = MessageMapping.build_properties(message_id, now, sender_id, attrs, system_attrs,
          group_id, dedup_id, delay.to_i64 * 1000, md5_of_body)
        exchange = ""
        if delay > 0
          ensure_delayed_binding(q.name)
          exchange = DELAYED_EXCHANGE
        end
        size = body.bytesize.to_u64
        msg = Message.new(now, exchange, q.name, props, size, IO::Memory.new(body.to_slice, writable: false))
        result = @vhost.publish(msg)
        @vhost.event_tick(EventType::ClientPublish)
        @vhost.add_recv_bytes(size)
        unless result.routed?
          # A FIFO duplicate is silently dropped by the deduplicating queue and
          # is a success to the client; only a vanished queue is an error
          raise QueueDoesNotExist.new if queue?(q.name).nil?
        end
        if result.rejected?
          # Not stored: a closed queue (e.g. during shutdown) is transient and
          # SDKs retry an InternalError; otherwise a reject-publish overflow
          raise InternalError.new if q.closed?
          raise OverLimit.new("The queue #{q.name} rejected the message because it is full.")
        end
        add_messages_sent
        SendResult.new(message_id, md5_of_body,
          Checksums.md5_attributes(attrs), Checksums.md5_attributes(system_attrs))
      end

      # Hands out up to `max` messages. Waits up to `wait` for the first one
      # (long polling) by blocking on the queue's not-empty signal.
      def receive(q : AMQP::Queue, max : Int32, wait : Time::Span, visibility_timeout : Time::Span) : Array(ReceivedMessage)
        inflight = inflight_for(q)
        messages = Array(ReceivedMessage).new(max)
        deadline = Time.instant + wait
        loop do
          while messages.size < max
            got = q.basic_get(false) do |env|
              now = RoughTime.unix_ms
              received = MessageMapping::Received.from(env, q.name)
              handle, entry = inflight.add(env.segment_position, received.message_id, visibility_timeout, now)
              @vhost.event_tick(EventType::ClientGet)
              @vhost.add_send_bytes(env.message.bodysize)
              messages << ReceivedMessage.new(handle, received, entry.receive_count, entry.first_receive_ts)
              add_messages_received
            end
            break unless got
          end
          break unless messages.empty?
          # Nothing handed out although the queue holds messages (paused, or
          # racing with other receivers); let the client poll again
          break unless q.empty.value
          remaining = deadline - Time.instant
          break if remaining <= Time::Span.zero
          timed_out = Metrics.long_polling do
            select
            when q.empty.when_false.receive then false
            when timeout remaining then true
            end
          end
          break if timed_out || q.closed?
        end
        messages
      end

      def delete_message(q : AMQP::Queue, handle : String) : Nil
        sp = inflight_for(q).delete(handle) || raise ReceiptHandleIsInvalid.new(
          "The input receipt handle \"#{handle}\" is not a valid receipt handle.")
        q.ack(sp)
        @vhost.event_tick(EventType::ClientAck)
        add_messages_deleted
      end

      def change_visibility(q : AMQP::Queue, handle : String, visibility_timeout : Time::Span) : Nil
        inflight = inflight_for(q)
        if visibility_timeout.zero?
          sp = inflight.release(handle) || raise ReceiptHandleIsInvalid.new(
            "The input receipt handle \"#{handle}\" is not a valid receipt handle.")
          q.reject(sp, true)
          @vhost.event_tick(EventType::ClientReject)
        else
          inflight.change_visibility(handle, visibility_timeout) || raise ReceiptHandleIsInvalid.new(
            "The input receipt handle \"#{handle}\" is not a valid receipt handle.")
        end
      end

      def purge(q : AMQP::Queue) : Nil
        now = Time.instant
        @lock.synchronize do
          if (last = @purged_at[q.name]?) && now - last < PURGE_INTERVAL
            raise PurgeQueueInProgress.new
          end
          @purged_at[q.name] = now
        end
        q.purge
      end

      def inflight_count(q : AMQP::Queue) : Int32
        @lock.synchronize { @inflights[q.name]?.try(&.size) } || 0
      end

      # Messages received and neither deleted nor timed out, over all queues
      def inflight_count : Int32
        @lock.synchronize { @inflights.sum { |_, i| i.size } }
      end

      def close : Nil
        @lock.synchronize do
          @inflights.each_value &.close
          @inflights.clear
        end
      end

      private def inflight_for(q : AMQP::Queue) : Inflight
        @lock.synchronize do
          if inflight = @inflights[q.name]?
            # A queue deleted and re-declared under the same name is a new object
            return inflight if inflight.queue.same?(q)
            inflight.close
          end
          @inflights[q.name] = Inflight.new(q) { |count| add_visibility_timeouts(count) }
        end
      end

      # Delayed messages are published through an internal delayed-message
      # exchange that routes to the queue by name once the delay has passed.
      # Binding is idempotent (an existing binding is neither re-added nor
      # re-stored), so this is cheap to call on every delayed send.
      private def ensure_delayed_binding(queue_name : String) : Nil
        unless @vhost.exchange?(DELAYED_EXCHANGE)
          @vhost.declare_exchange(DELAYED_EXCHANGE, "x-delayed-message", true, false, false,
            AMQP::Table.new({"x-delayed-type": "direct"}))
        end
        @vhost.bind_queue(queue_name, DELAYED_EXCHANGE, queue_name)
      end
    end
  end
end

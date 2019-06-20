require "logger"
require "./channel/consumer"
require "../amqp"
require "../stats"
require "../sortable_json"

module AvalancheMQ
  abstract class Client
    class Channel
      include Stats
      include SortableJSON

      getter id, client, prefetch_size, prefetch_count, global_prefetch,
        confirm, log, consumers, name
      property? running = true
      getter? client_flow

      @next_publish_exchange_name : String?
      @next_publish_routing_key : String?
      @next_msg_size = 0_u64
      @next_msg_props : AMQP::Properties?
      @next_msg_body = IO::Memory.new(4096)
      @log : Logger
      @client_flow = true

      rate_stats(%w(ack get publish deliver redeliver reject confirm return_unroutable))
      property deliver_count, redeliver_count

      def initialize(@client : Client, @id : UInt16)
        @log = @client.log.dup
        @log.progname += " channel=#{@id}"
        @name = "#{@client.channel_name_prefix}[#{@id}]"
        @prefetch_size = 0_u32
        @prefetch_count = 0_u16
        @confirm_total = 0_u64
        @confirm = false
        @global_prefetch = false
        @next_publish_mandatory = false
        @next_publish_immediate = false
        @consumers = Array(Consumer).new
        @delivery_tag = 0_u64
        @map = {} of UInt64 => Tuple(Queue, SegmentPosition, Consumer | Nil)
      end

      def details_tuple
        {
          number:                  @id,
          name:                    @name,
          vhost:                   @client.vhost.name,
          user:                    @client.user.try(&.name),
          consumer_count:          @consumers.size,
          prefetch_count:          @prefetch_count,
          global_prefetch_count:   @global_prefetch ? @prefetch_count : 0,
          confirm:                 @confirm,
          transactional:           false,
          messages_unacknowledged: @map.size,
          connection_details:      @client.connection_details,
          state:                   state,
          message_stats:           stats_details,
        }
      end

      def client_flow(active : Bool)
        @client_flow = active
        return unless active
        @consumers.each { |c| c.queue.consumer_available }
      end

      def state
        !@running ? "closed" : (@client.vhost.flow? ? "running" : "flow")
      end

      def send(frame)
        @client.send frame if @running
      end

      def confirm_select(frame)
        @confirm = true
        unless frame.no_wait
          send AMQP::Frame::Confirm::SelectOk.new(frame.channel)
        end
      end

      def start_publish(frame)
        unless server_flow?
          @client.send_precondition_failed(frame, "Server low on disk space")
          return
        end
        @next_publish_exchange_name = frame.exchange
        @next_publish_routing_key = frame.routing_key
        @next_publish_mandatory = frame.mandatory
        @next_publish_immediate = frame.immediate
        unless @client.vhost.exchanges[@next_publish_exchange_name]?
          msg = "No exchange '#{@next_publish_exchange_name}' in vhost '#{@client.vhost.name}'"
          @client.send_not_found(frame, msg)
        end
      end

      MAX_MESSAGE_BODY_SIZE = 512 * 1024 * 1024

      def next_msg_headers(frame)
        if direct_reply_request?(frame.properties.reply_to)
          if @client.direct_reply_channel
            frame.properties.reply_to = "#{DIRECT_REPLY_PREFIX}.#{@client.direct_reply_consumer_tag}"
          else
            @client.send_precondition_failed(frame, "Direct reply consumer does not exist")
            return
          end
        end
        if frame.body_size > MAX_MESSAGE_BODY_SIZE
          error = "message size #{frame.body_size} larger than max size #{MAX_MESSAGE_BODY_SIZE}"
          @client.send_precondition_failed(frame, error)
          return
        end
        @next_msg_size = frame.body_size
        @next_msg_props = frame.properties
        finish_publish(@next_msg_body) if frame.body_size.zero?
      end

      def add_content(frame)
        IO.copy(frame.body, @next_msg_body, frame.body_size)
        if @next_msg_body.pos == @next_msg_size
          @next_msg_body.rewind
          finish_publish(@next_msg_body)
        end
      end

      private def server_flow?
        @client.vhost.flow?
      end

      private def finish_publish(message_body)
        @publish_count += 1
        ts = Time.utc_now
        props = @next_msg_props.not_nil!
        props.timestamp = ts if Config.instance.set_timestamp && props.timestamp.nil?
        msg = Message.new(ts.to_unix_ms,
          @next_publish_exchange_name.not_nil!,
          @next_publish_routing_key.not_nil!,
          props,
          @next_msg_size,
          message_body)
        if msg.routing_key.starts_with?(DIRECT_REPLY_PREFIX)
          consumer_tag = msg.routing_key.lchop("#{DIRECT_REPLY_PREFIX}.")
          @client.vhost.direct_reply_channels[consumer_tag]?.try do |ch|
            deliver = AMQP::Frame::Basic::Deliver.new(ch.id, consumer_tag, 1_u64, false,
              msg.exchange_name, msg.routing_key)
            ch.deliver(deliver, msg)
            return true
          end
        end
        if @client.vhost.publish(msg, immediate: @next_publish_immediate)
          confirm_ack
        else
          @return_unroutable_count += 1
          if @next_publish_immediate
            retrn = AMQP::Frame::Basic::Return.new(@id, 313_u16, "NO_CONSUMERS", msg.exchange_name, msg.routing_key)
            deliver(retrn, msg)
          elsif @next_publish_mandatory
            retrn = AMQP::Frame::Basic::Return.new(@id, 312_u16, "NO_ROUTE", msg.exchange_name, msg.routing_key)
            deliver(retrn, msg)
          end
          confirm_nack
        end
      rescue ex
        @log.warn { "Could not handle message #{ex.inspect}" }
        confirm_nack
        raise ex
      ensure
        @next_msg_size = 0_u64
        @next_msg_body.clear
        @next_msg_props = nil
        @next_publish_exchange_name = @next_publish_routing_key = nil
        @next_publish_mandatory = @next_publish_immediate = false
      end

      private def confirm_nack
        return unless @confirm
        @confirm_total += 1
        send AMQP::Frame::Basic::Nack.new(@id, @confirm_total, false, false)
      end

      private def confirm_ack
        return unless @confirm
        @confirm_total += 1
        @confirm_count += 1 # Stats
        send AMQP::Frame::Basic::Ack.new(@id, @confirm_total, false)
      end

      def deliver(frame, msg)
        @client.deliver(frame, msg)
      end

      def consume(frame)
        if frame.consumer_tag.empty?
          frame.consumer_tag = "amq.ctag-#{Random::Secure.urlsafe_base64(24)}"
        end
        if direct_reply_request?(frame.queue)
          unless frame.no_ack
            @client.send_precondition_failed(frame, "Direct replys must be consumed in no-ack mode")
            return
          end
          @log.debug { "Saving direct reply consumer #{frame.consumer_tag}" }
          @client.direct_reply_consumer_tag = frame.consumer_tag
          @client.vhost.direct_reply_channels[frame.consumer_tag] = self
        elsif q = @client.vhost.queues[frame.queue]? || nil
          if q.exclusive && !@client.exclusive_queues.includes? q
            @client.send_resource_locked(frame, "Exclusive queue")
            return
          end
          if q.has_exclusive_consumer?
            @client.send_access_refused(frame, "Queue '#{frame.queue}' in vhost '#{@client.vhost.name}' in exclusive use")
            return
          end
          c = Consumer.new(self, frame.consumer_tag, q, frame.no_ack, frame.exclusive)
          @consumers.push(c)
          q.add_consumer(c)
          q.last_get_time = Time.utc_now.to_unix_ms
        else
          @client.send_not_found(frame, "Queue '#{frame.queue}' not declared")
        end
        unless frame.no_wait
          send AMQP::Frame::Basic::ConsumeOk.new(frame.channel, frame.consumer_tag)
        end
        Fiber.yield # Notify :add_consumer observers
      end

      def basic_get(frame)
        if q = @client.vhost.queues.fetch(frame.queue, nil)
          if q.exclusive && !@client.exclusive_queues.includes? q
            @client.send_resource_locked(frame, "Exclusive queue")
          else
            q.basic_get(frame.no_ack) do |env|
              if env
                delivery_tag = next_delivery_tag(q, env.segment_position, frame.no_ack, nil)
                get_ok = AMQP::Frame::Basic::GetOk.new(frame.channel, delivery_tag,
                  env.redelivered, env.message.exchange_name,
                  env.message.routing_key, q.message_count)
                deliver(get_ok, env.message)
                @redeliver_count += 1 if env.redelivered
              else
                send AMQP::Frame::Basic::GetEmpty.new(frame.channel)
              end
            end
          end
          q.last_get_time = Time.utc_now.to_unix_ms
          @get_count += 1
        else
          @client.send_not_found(frame, "No queue '#{frame.queue}' in vhost '#{@client.vhost.name}'")
          close
        end
      end

      def basic_ack(frame)
        if qspc = @map.delete(frame.delivery_tag)
          if frame.multiple
            tags = @map.select { |k, _| k < frame.delivery_tag }
            tags.each_value do |queue, sp, consumer|
              do_ack(frame, queue, sp, consumer, flush: false)
            end
            tags.each_key do |k|
              @map.delete(k)
            end
          end
          queue, sp, consumer = qspc
          do_ack(frame, queue, sp, consumer)
        else
          reply_text = "Unknown delivery tag '#{frame.delivery_tag}'"
          @client.send_precondition_failed(frame, reply_text)
        end
      end

      private def do_ack(frame, queue, sp, consumer, flush = true)
        consumer.ack(sp) if consumer
        queue.ack(sp, flush: flush)
        @ack_count += 1
      end

      def basic_reject(frame)
        if qspc = @map.delete(frame.delivery_tag)
          queue, sp, consumer = qspc
          do_reject(frame, queue, sp, consumer)
        else
          reply_text = "Unknown delivery tag '#{frame.delivery_tag}'"
          @client.send_precondition_failed(frame, reply_text)
        end
      end

      def basic_nack(frame)
        if frame.multiple && frame.delivery_tag.zero?
          @map.each_value do |queue, sp, consumer|
            do_reject(frame, queue, sp, consumer)
          end
          @map.clear
        elsif qspc = @map.delete(frame.delivery_tag)
          if frame.multiple
            tags = @map.select { |k, _| k < frame.delivery_tag }
            tags.each_value do |queue, sp, consumer|
              do_reject(frame, queue, sp, consumer)
            end
            tags.each_key do |k|
              @map.delete k
            end
          end
          queue, sp, consumer = qspc
          do_reject(frame, queue, sp, consumer)
        else
          reply_text = "Unknown delivery tag '#{frame.delivery_tag}'"
          @client.send_precondition_failed(frame, reply_text)
        end
      end

      private def do_reject(frame, queue, sp, consumer)
        consumer.reject(sp) if consumer
        queue.reject(sp, frame.requeue)
        @reject_count += 1
      end

      def basic_qos(frame)
        @prefetch_size = frame.prefetch_size
        @prefetch_count = frame.prefetch_count
        @global_prefetch = frame.global
        send AMQP::Frame::Basic::QosOk.new(frame.channel)
      end

      def basic_recover(frame)
        @consumers.each { |c| c.recover(frame.requeue) }
        @map.each_value { |queue, sp, consumer| queue.reject(sp, true) if consumer.nil? }
        @map.clear
        send AMQP::Frame::Basic::RecoverOk.new(frame.channel)
      end

      def close
        @running = false
        @consumers.each { |c| c.queue.rm_consumer(c) }
        @map.each_value do |queue, sp, consumer|
          if consumer.nil?
            queue.reject sp, true
          end
        end
        @log.debug { "Closed" }
      end

      def next_delivery_tag(queue : Queue, sp, no_ack, consumer) : UInt64
        tag = @delivery_tag += 1
        @map[tag] = {queue, sp, consumer} unless no_ack
        tag
      end

      def cancel_consumer(frame)
        @log.debug { "Canceling consumer '#{frame.consumer_tag}'" }
        if c = @consumers.find { |conn| conn.tag == frame.consumer_tag }
          c.queue.rm_consumer(c)
          unless frame.no_wait
            send AMQP::Frame::Basic::CancelOk.new(frame.channel, frame.consumer_tag)
          end
        else
          unless frame.no_wait
            send AMQP::Frame::Basic::CancelOk.new(frame.channel, frame.consumer_tag)
          end
        end
      end

      DIRECT_REPLY_PREFIX = "amq.direct.reply-to"

      def direct_reply_request?(str)
        # no regex for speed
        str.try { |r| r == "amq.rabbitmq.reply-to" || r == DIRECT_REPLY_PREFIX }
      end
    end
  end
end

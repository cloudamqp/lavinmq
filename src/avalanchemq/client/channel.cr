require "logger"
require "./channel/consumer"
require "../amqp"

module AvalancheMQ
  abstract class Client
    class Channel
      getter id, client, prefetch_size, prefetch_count, global_prefetch,
        confirm, log, consumers, name
      property? running = true

      @next_publish_exchange_name : String?
      @next_publish_routing_key : String?
      @next_msg_size = 0_u64
      @next_msg_props : AMQP::Properties?
      @next_msg_body = IO::Memory.new
      @log : Logger

      def initialize(@client : Client, @id : UInt16)
        @log = @client.log.dup
        @log.progname += " channel=#{@id}"
        @name = "#{@client.channel_name_prefix}[#{@id}]"
        @prefetch_size = 0_u32
        @prefetch_count = 0_u16
        @confirm_count = 0_u64
        @confirm = false
        @global_prefetch = false
        @next_publish_mandatory = false
        @next_publish_immediate = false
        @consumers = Array(Consumer).new
        @delivery_tag = 0_u64
        @map = {} of UInt64 => Tuple(Queue, SegmentPosition, Consumer | Nil)
      end

      def details
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
          state:                   @running ? "running" : "closed",
        }
      end

      def to_json(builder : JSON::Builder)
        details.to_json(builder)
      end

      def send(frame)
        @client.send frame
      end

      def confirm_select(frame)
        @confirm = true
        unless frame.no_wait
          @client.send AMQP::Confirm::SelectOk.new(frame.channel)
        end
      end

      def start_publish(frame)
        @log.debug { "Start publish #{frame.inspect}" }
        @next_publish_exchange_name = frame.exchange
        @next_publish_routing_key = frame.routing_key
        @next_publish_mandatory = frame.mandatory
        @next_publish_immediate = frame.immediate
      end

      def next_msg_headers(frame)
        @log.debug { "Next msg headers: #{frame.inspect}" }
        if direct_reply_request?(frame.properties.reply_to)
          if @client.direct_reply_channel
            frame.properties.reply_to = "#{DIRECT_REPLY_PREFIX}.#{@client.direct_reply_consumer_tag}"
          else
            @client.send_precondition_failed(frame, "Direct reply consumer does not exist")
            return
          end
        end
        @next_msg_size = frame.body_size
        @next_msg_props = frame.properties
        finish_publish(@next_msg_body) if frame.body_size.zero?
      end

      def add_content(frame)
        @log.debug { "Adding content #{frame.inspect}" }
        if frame.body_size == @next_msg_size
          finish_publish(frame.body)
        else
          IO.copy(frame.body, @next_msg_body, frame.body_size)
          if @next_msg_body.pos == @next_msg_size
            @next_msg_body.rewind
            finish_publish(@next_msg_body)
          end
        end
      end

      private def finish_publish(message_body)
        @log.debug { "Finishing publish #{message_body.inspect}" }
        delivered = false
        ts = Time.utc_now
        props = @next_msg_props.not_nil!
        props.timestamp = ts unless props.timestamp
        msg = Message.new(ts.epoch_ms,
          @next_publish_exchange_name.not_nil!,
          @next_publish_routing_key.not_nil!,
          props,
          @next_msg_size,
          message_body)
        if msg.routing_key.starts_with?(DIRECT_REPLY_PREFIX)
          consumer_tag = msg.routing_key.lchop("#{DIRECT_REPLY_PREFIX}.")
          @client.vhost.direct_reply_channels[consumer_tag]?.try do |ch|
            deliver = AMQP::Basic::Deliver.new(ch.id, consumer_tag, 1_u64, false,
              msg.exchange_name, msg.routing_key)
            ch.deliver(deliver, msg)
            delivered = true
          end
        end
        delivered ||= @client.vhost.publish(msg, immediate: @next_publish_immediate)
        unless delivered
          if @next_publish_immediate
            retrn = AMQP::Basic::Return.new(@id, 313_u16, "No consumers", msg.exchange_name, msg.routing_key)
            deliver(retrn, msg)
          elsif @next_publish_mandatory
            retrn = AMQP::Basic::Return.new(@id, 312_u16, "No Route", msg.exchange_name, msg.routing_key)
            deliver(retrn, msg)
          else
            @log.debug "Skipping body"
            message_body.skip(@next_msg_size)
          end
        end
        if @confirm
          @confirm_count += 1
          @client.send AMQP::Basic::Ack.new(@id, @confirm_count, false)
        end
      rescue ex
        @client.send AMQP::Basic::Nack.new(@id, @confirm_count, false, false) if @confirm
        raise ex
      ensure
        @next_msg_size = 0_u64
        @next_msg_body.clear
        @next_publish_exchange_name = @next_publish_routing_key = nil
        @next_publish_mandatory = @next_publish_immediate = false
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
            @client.send_access_refused(frame, "queue '#{frame.queue}' in vhost '#{@client.vhost.name}' in exclusive use")
            return
          end
          c = Consumer.new(self, frame.consumer_tag, q, frame.no_ack, frame.exclusive)
          @consumers.push(c)
          q.add_consumer(c)
          q.last_get_time = Time.utc_now.epoch_ms
        else
          @client.send_not_found(frame, "Queue '#{frame.queue}' not declared")
        end
        unless frame.no_wait
          @client.send AMQP::Basic::ConsumeOk.new(frame.channel, frame.consumer_tag)
        end
      end

      def basic_get(frame)
        if q = @client.vhost.queues.fetch(frame.queue, nil)
          if q.exclusive && !@client.exclusive_queues.includes? q
            @client.send_resource_locked(frame, "Exclusive queue")
          elsif env = q.get(frame.no_ack)
            delivery_tag = next_delivery_tag(q, env.segment_position, frame.no_ack, nil)
            get_ok = AMQP::Basic::GetOk.new(frame.channel, delivery_tag,
              env.redelivered, env.message.exchange_name,
              env.message.routing_key, q.message_count)
            deliver(get_ok, env.message)
          else
            @client.send AMQP::Basic::GetEmpty.new(frame.channel)
          end
          q.last_get_time = Time.utc_now.epoch_ms
        else
          reply_code = "NOT_FOUND - no queue '#{frame.queue}' in vhost '#{@client.vhost.name}'"
          @client.close_channel(frame, 404_u16, reply_code)
          close
        end
      end

      def basic_ack(frame)
        if qspc = @map.delete(frame.delivery_tag)
          if frame.multiple
            @map.select { |k, _| k < frame.delivery_tag }
              .each_value do |queue, sp, consumer|
                consumer.ack(sp) if consumer
                queue.ack(sp, flush: false)
              end
            @map.delete_if { |k, _| k < frame.delivery_tag }
          end
          queue, sp, consumer = qspc
          consumer.ack(sp) if consumer
          queue.ack(sp, flush: true)
        else
          reply_text = "unknown delivery tag #{frame.delivery_tag}"
          @client.send_precondition_failed(frame, reply_text)
        end
      end

      def basic_reject(frame)
        if qspc = @map.delete(frame.delivery_tag)
          queue, sp, consumer = qspc
          consumer.reject(sp) if consumer
          queue.reject(sp, frame.requeue)
        else
          reply_text = "unknown delivery tag #{frame.delivery_tag}"
          @client.send_precondition_failed(frame, reply_text)
        end
      end

      def basic_nack(frame)
        if frame.multiple && frame.delivery_tag.zero?
          @map.each_value do |queue, sp, consumer|
            consumer.reject(sp) if consumer
            queue.reject(sp, frame.requeue)
          end
          @map.clear
        elsif qspc = @map.delete(frame.delivery_tag)
          if frame.multiple
            @map.select { |k, _| k < frame.delivery_tag }
              .each_value do |queue, sp, consumer|
                consumer.reject(sp) if consumer
                queue.reject(sp, frame.requeue)
              end
            @map.delete_if { |k, _| k < frame.delivery_tag }
          end
          queue, sp, consumer = qspc
          consumer.reject(sp) if consumer
          queue.reject(sp, frame.requeue)
        else
          reply_text = "unknown delivery tag #{frame.delivery_tag}"
          @client.send_precondition_failed(frame, reply_text)
        end
      end

      def basic_qos(frame)
        @prefetch_size = frame.prefetch_size
        @prefetch_count = frame.prefetch_count
        @global_prefetch = frame.global
        @client.send AMQP::Basic::QosOk.new(frame.channel)
      end

      def close
        @log.debug { "Closing" }
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
        @delivery_tag += 1
        @map[@delivery_tag] = {queue, sp, consumer} unless no_ack
        @delivery_tag
      end

      def cancel_consumer(frame)
        @log.debug { "Canceling consumer #{frame.consumer_tag}" }
        if c = @consumers.find { |conn| conn.tag == frame.consumer_tag }
          c.queue.rm_consumer(c)
          unless frame.no_wait
            @client.send AMQP::Basic::CancelOk.new(frame.channel, frame.consumer_tag)
          end
        else
          # text = "No consumer for tag #{frame.consumer_tag} on channel #{frame.channel}"
          # @client.send AMQP::Channel::Close.new(frame.channel, 406_u16, text, frame.class_id, frame.method_id)
          unless frame.no_wait
            @client.send AMQP::Basic::CancelOk.new(frame.channel, frame.consumer_tag)
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

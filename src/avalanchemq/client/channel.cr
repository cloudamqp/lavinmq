require "logger"
require "./channel/consumer"
require "../amqp"

module AvalancheMQ
  class Client
    class Channel
      getter id, client, prefetch_size, prefetch_count, global_prefetch,
        confirm, log

      @next_publish_exchange_name : String?
      @next_publish_routing_key : String?
      @next_msg_size = 0_u64
      @next_msg_props : AMQP::Properties?
      @next_msg_body = IO::Memory.new
      @log : Logger

      def initialize(@client : Client, @id : UInt16)
        @log = @client.log.dup
        @log.progname += "/Channel[#{@id}]"
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
        @next_publish_exchange_name = frame.exchange
        @next_publish_routing_key = frame.routing_key
        @next_publish_mandatory = frame.mandatory
        @next_publish_immediate = frame.immediate
      end

      def next_msg_headers(frame)
        @next_msg_size = frame.body_size
        @next_msg_props = frame.properties
        finish_publish(frame) if frame.body_size.zero?
      end

      def add_content(frame)
        bytes = frame.body
        raise "No msg to write to" if @next_msg_body.nil?
        @next_msg_body.not_nil!.write bytes
        if @next_msg_body.not_nil!.pos == @next_msg_size.not_nil!
          finish_publish(frame)
        end
      end

      private def finish_publish(frame)
        msg = Message.new(Time.now.epoch_ms,
                          @next_publish_exchange_name.not_nil!,
                          @next_publish_routing_key.not_nil!,
                          @next_msg_props.not_nil!,
                          @next_msg_size.not_nil!,
                          @next_msg_body.not_nil!.to_slice)
        routed = false
        begin
          routed = @client.vhost.publish(msg, immediate: @next_publish_immediate)
        rescue NoImmediateDeliveryError | MessageUnroutableError
          if @next_publish_immediate
            @client.send AMQP::Basic::Return.new(frame.channel, 313_u16, "No consumers",
                                                 msg.exchange_name, msg.routing_key)
            deliver(msg)
          elsif @next_publish_mandatory
            @client.send AMQP::Basic::Return.new(frame.channel, 312_u16, "No Route",
                                                 msg.exchange_name, msg.routing_key)
            deliver(msg)
          end
        end
        if @confirm
          @confirm_count += 1
          if routed
            @client.send AMQP::Basic::Ack.new(frame.channel, @confirm_count, false)
          else
            @client.send AMQP::Basic::Nack.new(frame.channel, @confirm_count, false, false)
          end
        end

        @next_msg_body.not_nil!.clear
        @next_publish_exchange_name = @next_publish_routing_key = nil
        @next_publish_mandatory = @next_publish_immediate = false
      end

      def consume(frame)
        q = @client.vhost.queues[frame.queue]
        if q.exclusive && !@client.exclusive_queues.includes? q
          @client.send_resource_locked(frame, "Exclusive queue")
          return
        end
        if q.has_exclusive_consumer?
          @client.send_resource_locked(frame, "Queue has an exclusive consumer")
          return
        end
        if frame.consumer_tag.empty?
          frame.consumer_tag = "amq.ctag-#{Random::Secure.urlsafe_base64(24)}"
        end
        c = Consumer.new(self, frame.consumer_tag, q, frame.no_ack, frame.exclusive)
        unless frame.no_wait
          @client.send AMQP::Basic::ConsumeOk.new(frame.channel, frame.consumer_tag)
        end
        @consumers.push(c)
        q.add_consumer(c)
      end

      def basic_get(frame)
        if q = @client.vhost.queues.fetch(frame.queue, nil)
          if q.exclusive && !@client.exclusive_queues.includes? q
            @client.send_resource_locked(frame, "Exclusive queue")
          elsif env = q.get(frame.no_ack)
            delivery_tag = next_delivery_tag(q, env.segment_position, frame.no_ack, nil)
            @client.send AMQP::Basic::GetOk.new(frame.channel, delivery_tag,
                                                false, env.message.exchange_name,
                                                env.message.routing_key, q.message_count)
            deliver(env.message)
          else
            @client.send AMQP::Basic::GetEmpty.new(frame.channel)
          end
        else
          reply_code = "NOT_FOUND - no queue '#{frame.queue}' in vhost '#{@client.vhost.name}'"
          @client.send AMQP::Channel::Close.new(frame.channel, 404_u16, reply_code, frame.class_id, frame.method_id)
          close
        end
      end

      def deliver(msg)
        @log.debug { "Sending HeaderFrame" }
        @client.send AMQP::HeaderFrame.new(@id, 60_u16, 0_u16, msg.size, msg.properties)
        pos = 0
        while pos < msg.size
          length = [msg.size - pos, @client.max_frame_size - 8].min
          body_part = msg.body[pos, length]
          @log.debug { "Sending BodyFrame (pos #{pos}, length #{length})" }
          @client.send AMQP::BodyFrame.new(@id, body_part)
          pos += length
        end
      end

      def basic_ack(frame)
        if qspc = @map.delete(frame.delivery_tag)
          if frame.multiple
            @map.select { |k, _| k < frame.delivery_tag }.
              each_value do |queue, sp, consumer|
              consumer.ack(sp) if consumer
              queue.ack(sp, flush: false)
            end
            @map.delete_if { |k, _| k < frame.delivery_tag }
          end
          queue, sp, consumer = qspc
          consumer.ack(sp) if consumer
          queue.ack(sp, flush: true)
        else
          reply_text = "Unknown delivery tag #{frame.delivery_tag}"
          @client.send_precondition_failed(frame, reply_text)
        end
      end

      def basic_reject(frame)
        if qspc = @map.delete(frame.delivery_tag)
          queue, sp, consumer = qspc
          consumer.reject(sp) if consumer
          queue.reject(sp, frame.requeue)
        else
          reply_text = "Unknown delivery tag #{frame.delivery_tag}"
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
            @map.select { |k, _| k < frame.delivery_tag }.
              each_value do |queue, sp, consumer|
              consumer.reject(sp) if consumer
              queue.reject(sp, frame.requeue)
            end
            @map.delete_if { |k, _| k < frame.delivery_tag }
          end
          queue, sp, consumer = qspc
          consumer.reject(sp) if consumer
          queue.reject(sp, frame.requeue)
        else
          reply_text = "Unknown delivery tag #{frame.delivery_tag}"
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
        @map[@delivery_tag] = { queue, sp, consumer } unless no_ack
        @delivery_tag
      end

      def cancel_consumer(frame)
        @log.debug { "Canceling consumer #{frame.consumer_tag}" }
        if c = @consumers.find { |c| c.tag == frame.consumer_tag }
          c.queue.rm_consumer(c)
          unless frame.no_wait
            @client.send AMQP::Basic::CancelOk.new(frame.channel, frame.consumer_tag)
          end
        else
          text = "No consumer for tag #{frame.consumer_tag} on channel #{frame.channel}"
          @client.send AMQP::Channel::Close.new(frame.channel, 406_u16, text,
                                                frame.class_id, frame.method_id)
        end
      end
    end
  end
end

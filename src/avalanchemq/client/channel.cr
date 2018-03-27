module AvalancheMQ
  class Client
    class Channel
      getter prefetch_size, prefetch_count, global_prefetch, confirm

      @next_publish_exchange_name : String | Nil
      @next_publish_routing_key : String | Nil
      @next_msg_body : IO::Memory = IO::Memory.new

      def send(frame)
        @client.send frame
      end

      def initialize(@client : Client)
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

      def log
        @client.log
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

      def next_msg_headers(size : UInt64, props : AMQP::Properties)
        @next_msg_size = size
        @next_msg_props = props
      end

      def add_content(frame)
        bytes = frame.body
        raise "No msg to write to" if @next_msg_body.nil?
        @next_msg_body.not_nil!.write bytes
        if @next_msg_body.not_nil!.pos == @next_msg_size.not_nil!
          msg = Message.new(Time.now.epoch_ms,
                            @next_publish_exchange_name.not_nil!,
                            @next_publish_routing_key.not_nil!,
                            @next_msg_props.not_nil!,
                            @next_msg_size.not_nil!,
                            @next_msg_body.not_nil!.to_slice)
          routed = @client.vhost.publish(msg, immediate: @next_publish_immediate)
          if !routed && @next_publish_immediate
            @client.send AMQP::Basic::Return.new(frame.channel, 313_u16, "No consumers",
                                               msg.exchange_name, msg.routing_key)
          elsif !routed && @next_publish_mandatory
            @client.send AMQP::Basic::Return.new(frame.channel, 312_u16, "No Route",
                                                 msg.exchange_name, msg.routing_key)
          end
          if @confirm
            @confirm_count += 1
            @client.send AMQP::Basic::Ack.new(frame.channel, @confirm_count, false)
          end

          @next_msg_body.not_nil!.clear
          @next_publish_exchange_name = @next_publish_routing_key = nil
          @next_publish_mandatory = @next_publish_immediate = false
        end
      end

      def consume(frame)
        q = @client.vhost.queues[frame.queue]
        if frame.consumer_tag.empty?
          frame.consumer_tag = "amq.ctag-#{Random::Secure.urlsafe_base64(24)}"
        end
        c = Consumer.new(self, frame.channel, frame.consumer_tag, q, frame.no_ack)
        unless frame.no_wait
          @client.send AMQP::Basic::ConsumeOk.new(frame.channel, frame.consumer_tag)
        end
        @consumers.push(c)
        q.add_consumer(c)
      end

      def basic_get(frame)
        if q = @client.vhost.queues.fetch(frame.queue, nil)
          if env = q.get(frame.no_ack)
            delivery_tag = next_delivery_tag(q, env.segment_position, frame.no_ack, nil)
            @client.send AMQP::Basic::GetOk.new(frame.channel, delivery_tag,
                                                false, env.message.exchange_name,
                                                env.message.routing_key, q.message_count)
            @client.send AMQP::HeaderFrame.new(frame.channel, 60_u16, 0_u16,
                                               env.message.size, env.message.properties)
            @client.send AMQP::BodyFrame.new(frame.channel, env.message.body.to_slice)
          else
            @client.send AMQP::Basic::GetEmpty.new(frame.channel)
          end
        else
          reply_code = "NOT_FOUND - no queue '#{frame.queue}' in vhost '#{@client.vhost.name}'"
          @client.send AMQP::Channel::Close.new(frame.channel, 404_u16, reply_code, frame.class_id, frame.method_id)
          close
        end
      end

      def basic_ack(frame)
        if frame.multiple
          @map.select { |k, _| k <= frame.delivery_tag }.each_value do |queue, sp, consumer|
            consumer.ack(sp) if consumer
            queue.ack(sp)
          end
          @map.delete_if { |k, _| k <= frame.delivery_tag }
        elsif qspc = @map.delete(frame.delivery_tag)
          queue, sp, consumer = qspc
          consumer.ack(sp) if consumer
          queue.ack(sp)
        else
          reply_code = "No matching delivery tag on this channel"
          @client.send AMQP::Channel::Close.new(frame.channel, 404_u16, reply_code,
                                                frame.class_id, frame.method_id)
          close
        end
      end

      def basic_reject(frame)
        if qspc = @map.delete(frame.delivery_tag)
          queue, sp, consumer = qspc
          consumer.reject(sp) if consumer
          queue.reject(sp, frame.requeue)
        else
          reply_code = "No matching delivery tag on this channel"
          @client.send AMQP::Channel::Close.new(frame.channel, 404_u16, reply_code,
                                                frame.class_id, frame.method_id)
          close
        end
      end

      def basic_nack(frame)
        if frame.multiple
          if frame.delivery_tag.zero?
            @map.each_value do |queue, sp, consumer|
              consumer.reject(sp) if consumer
              queue.reject(sp, frame.requeue)
            end
            @map.clear
          else
            @map.select { |k, _| k <= frame.delivery_tag }.each_value do |queue, sp, consumer|
              consumer.reject(sp) if consumer
              queue.reject(sp, frame.requeue)
            end
            @map.delete_if { |k, _| k <= frame.delivery_tag }
          end
        elsif qspc = @map.delete(frame.delivery_tag)
          queue, sp, consumer = qspc
          consumer.reject(sp) if consumer
          queue.reject(sp, frame.requeue)
        else
          reply_code = "No matching delivery tag on this channel"
          @client.send AMQP::Channel::Close.new(frame.channel, 404_u16, reply_code,
                                                frame.class_id, frame.method_id)
          close
        end
      end

      def basic_qos(frame)
        @prefetch_size = frame.prefetch_size
        @prefetch_count = frame.prefetch_count
        @global_prefetch = frame.global
        @client.send AMQP::Basic::QosOk.new(frame.channel)
      end

      def close
        @consumers.each { |c| c.queue.rm_consumer(c) }
        @consumers.clear
        @map.each_value do |queue, sp, consumer|
          consumer.reject sp if consumer
          queue.reject sp, true
        end
        @map.clear
      end

      def next_delivery_tag(queue : Queue, sp, no_ack, consumer) : UInt64
        @delivery_tag += 1
        @map[@delivery_tag] = { queue, sp, consumer } unless no_ack
        @delivery_tag
      end

      class Consumer
        getter no_ack, queue, unacked
        def initialize(@channel : Client::Channel, @channel_id : UInt16,
                       @tag : String, @queue : Queue, @no_ack : Bool)
          @unacked = Set(SegmentPosition).new
        end

        def accepts?
          @channel.prefetch_count.zero? || @unacked.size < @channel.prefetch_count
        end

        def deliver(msg, sp, queue, redelivered = false)
          @unacked << sp unless @no_ack

          @channel.log.debug { "Getting delivery tag" }
          delivery_tag = @channel.next_delivery_tag(queue, sp, @no_ack, self)
          @channel.log.debug { "Delivering to consumer #{@tag}" }
          @channel.send AMQP::Basic::Deliver.new(@channel_id, @tag,
                                                 delivery_tag,
                                                 redelivered,
                                                 msg.exchange_name, msg.routing_key)
          @channel.log.debug { "HeaderFrame to consumer #{@tag}" }
          @channel.send AMQP::HeaderFrame.new(@channel_id, 60_u16, 0_u16, msg.size,
                                              msg.properties)
          # TODO: split body in FRAME_MAX sizes
          @channel.log.debug { "BodyFrame to consumer #{@tag}" }
          @channel.send AMQP::BodyFrame.new(@channel_id, msg.body)
          @channel.log.debug { "Sent all frames" }
        end

        def ack(sp)
          @unacked.delete(sp)
          @channel.log.debug { "Consumer #{@tag} acking #{sp}. Unacked: #{@unacked.size}" }
        end

        def reject(sp)
          @unacked.delete(sp)
          @channel.log.debug { "Consumer #{@tag} rejecting #{sp}. Unacked: #{@unacked.size}" }
        end
      end
    end
  end
end

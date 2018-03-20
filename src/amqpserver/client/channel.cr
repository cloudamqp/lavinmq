module AMQPServer
  class Client
    class Channel
      getter :client

      def initialize(@client : Client)
        @consumers = Array(Consumer).new
        @delivery_tag = 0_u64
        @map = {} of UInt64 => Tuple(SegmentPosition, Queue)
      end

      def start_publish(exchange_name : String, routing_key : String)
        @next_publish_exchange_name = exchange_name
        @next_publish_routing_key = routing_key
      end

      def next_msg_headers(size : UInt64, props : AMQP::Properties)
        @next_msg_size = size
        @next_msg_props = props
        @next_msg_body = IO::Memory.new(size)
      end

      def add_content(bytes)
        raise "No msg to write to" if @next_msg_body.nil?
        @next_msg_body.not_nil!.write bytes
        if @next_msg_body.not_nil!.pos == @next_msg_size.not_nil!
          msg = Message.new(@next_publish_exchange_name.not_nil!,
                            @next_publish_routing_key.not_nil!,
                            @next_msg_size.not_nil!,
                            @next_msg_props.not_nil!,
                            @next_msg_body.not_nil!.to_slice)
          @client.vhost.publish(msg)
          @next_msg_body.not_nil!.clear
          @next_msg_body = @next_publish_exchange_name = @next_publish_routing_key = nil
        end
      end

      def consume(frame)
        q = @client.vhost.queues[frame.queue]
        c = Consumer.new(self, frame.channel, frame.consumer_tag, q, frame.no_ack)
        @consumers.push(c)
        unless frame.no_wait
          @client.send AMQP::Basic::ConsumeOk.new(frame.channel, frame.consumer_tag)
        end
      end

      def basic_get(frame)
        if q = @client.vhost.queues.fetch(frame.queue, nil)
          if msg_sp = q.get(frame.no_ack)
            msg, sp = msg_sp
            delivery_tag = next_delivery_tag(sp, q)
            @client.send AMQP::Basic::GetOk.new(frame.channel, delivery_tag,
                                                false, msg.exchange_name,
                                                msg.routing_key, q.message_count)
            @client.send AMQP::HeaderFrame.new(frame.channel, 60_u16, 0_u16,
                                               msg.size, msg.properties)
            @client.send AMQP::BodyFrame.new(frame.channel, msg.body.to_slice)
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
        if spq = @map.delete(frame.delivery_tag)
          sp, queue = spq
          queue.ack(sp)
        else
          reply_code = "No matching delivery tag on this channel"
          @client.send AMQP::Channel::Close.new(frame.channel, 404_u16, reply_code,
                                                frame.class_id, frame.method_id)
          close
        end
      end

      def basic_reject(frame)
        if spq = @map.delete(frame.delivery_tag)
          sp, queue = spq
          queue.reject(sp)
        else
          reply_code = "No matching delivery tag on this channel"
          @client.send AMQP::Channel::Close.new(frame.channel, 404_u16, reply_code,
                                                frame.class_id, frame.method_id)
          close
        end
      end

      def close
        @consumers.each &.close
        @consumers.clear
        @map.each_value do |sp, queue|
          queue.reject sp
        end
        @map.clear
      end

      def next_delivery_tag(sp, queue : Queue) : UInt64
        @delivery_tag += 1
        @map[@delivery_tag] = { sp, queue }
        @delivery_tag
      end

      class Consumer
        getter :no_ack
        def initialize(@channel : Client::Channel, @channel_id : UInt16,
                       @tag : String, @queue : AMQPServer::Queue, @no_ack : Bool)
          @queue.add_consumer(self)
        end

        def close
          @queue.rm_consumer(self)
        end

        def deliver(msg, sp, queue, redelivered = false)
          @channel.client.send AMQP::Basic::Deliver.new(@channel_id, @tag,
                                                        @channel.next_delivery_tag(sp, queue),
                                                        redelivered,
                                                        msg.exchange_name, msg.routing_key)
          @channel.client.send AMQP::HeaderFrame.new(@channel_id, 60_u16, 0_u16, msg.size,
                                                     msg.properties)
          # TODO: split body in FRAME_MAX sizes
          @channel.client.send AMQP::BodyFrame.new(@channel_id, msg.body.to_slice)
        end
      end
    end
  end
end

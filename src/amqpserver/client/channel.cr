module AMQPServer
  class Client
    class Channel
      getter :client, :consumers

      def initialize(@client : Client)
        @consumers = Array(Consumer).new
        @delivery_tag = 0_u64
        @map = {} of UInt64 => Tuple(UInt64, Queue)
      end

      def start_publish(exchange_name : String, routing_key : String)
        @next_publish_exchange_name = exchange_name
        @next_publish_routing_key = routing_key
      end

      def next_msg_headers(size, props)
        @next_msg = Message.new(@next_publish_exchange_name.not_nil!,
                                @next_publish_routing_key.not_nil!, size, props)
      end

      def add_content(bytes)
        msg = @next_msg
        raise "No msg to write to" if msg.nil?
        msg << bytes
        @client.vhost.publish(msg) if msg.full?
      end

      def consume(frame)
        q = @client.vhost.queues[frame.queue]
        Consumer.new(self, frame.channel, frame.consumer_tag, q, frame.no_ack)
      end

      def basic_get(frame)
        if q = @client.vhost.queues.fetch(frame.queue, nil)
          msg, offset = q.get(frame.no_ack)
          if msg
            delivery_tag = next_delivery_tag(offset, q)
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
        if oq = @map.delete(frame.delivery_tag)
          offset, queue = oq
          queue.ack(offset)
        else
          reply_code = "No matching delivery tag on this channel"
          @client.send AMQP::Channel::Close.new(frame.channel, 404_u16, reply_code,
                                                frame.class_id, frame.method_id)
          close
        end
      end

      def close
        @consumers.each { |c| c.close }
      end

      def next_delivery_tag(offset : UInt64, queue : Queue) : UInt64
        @delivery_tag += 1
        @map[@delivery_tag] = { offset, queue }
        @delivery_tag
      end

      class Consumer
        getter :no_ack
        def initialize(@channel : Client::Channel, @channel_id : UInt16,
                       @tag : String, @queue : AMQPServer::Queue, @no_ack : Bool)
          @queue.add_consumer(self)
          @channel.consumers.push(self)
        end

        def close
          @queue.rm_consumer(self)
          @channel.consumers.delete(self)
        end

        def deliver(msg, offset, queue, redelivered = false)
          @channel.client.send AMQP::Basic::Deliver.new(@channel_id, @tag,
                                                @channel.next_delivery_tag(offset, queue),
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

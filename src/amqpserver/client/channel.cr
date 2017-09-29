module AMQPServer
  class Client
    class Channel
      getter :consumers
      def initialize(@client : Client)
        @consumers = Array(Consumer).new
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
        if msg.full?
          ex = @client.vhost.exchanges[msg.exchange_name]
          ex.publish(msg)
        end
      end

      def consume(consume_frame)
        q = @client.vhost.queues[consume_frame.queue]
        Consumer.new(@client, consume_frame.channel, consume_frame.consumer_tag, q)
      end

      def close
        @consumers.each { |c| c.close }
      end

      class Consumer
        def initialize(@client : Client, @channel : UInt16,
                       @tag : String, @queue : AMQPServer::Queue)
          @queue.add_consumer(self)
          @client.channels[@channel].consumers.push(self)
        end

        def close
          @queue.rm_consumer(self)
          @client.channels[@channel].consumers.delete(self)
        end

        def deliver(msg)
          @client.deliver @channel, @tag, false, msg
        end
      end
    end
  end
end

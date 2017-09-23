module AMQPServer
  class Client
    class Channel
      def initialize(@client : Client, @vhost : Server::State::VHost)
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
        send_msg_to_queue(msg) if msg.full?
      end

      private def send_msg_to_queue(msg)
        ex = @vhost.exchanges[msg.exchange_name]
        queues = ex.queues_matching(msg.routing_key)
        queues.each do |q|
          q.publish(msg)
        end
      end

      def consume(consume_frame)
        q = @vhost.queues[consume_frame.queue]
        c = Consumer.new(@client, consume_frame.channel, consume_frame.consumer_tag, q)
        c.register
        @consumers.push c
      end

      def stop
        @consumers.each { |c| c.deregister }
      end

      class Consumer
        def initialize(@client : Client, @channel : UInt16,
                       @tag : String, @queue : AMQPServer::Queue)
        end

        def register
          @queue.add_consumer(self)
        end

        def deregister
          @queue.rm_consumer(self)
        end

        def deliver(msg)
          @client.deliver @channel, @tag, msg
        end
      end
    end
  end
end

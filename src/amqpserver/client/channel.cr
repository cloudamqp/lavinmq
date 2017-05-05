module AMQPServer
  class Client
    class Channel
      def initialize(@state : Server::State)
        @publish_queues = Array(Queue).new
      end

      def start_publish(exchange_name, routing_key)
        @next_msg = Message.new exchange_name, routing_key
      end

      def next_msg_body_size(size)
        raise "No msg to write to" if @next_msg.nil?
        @next_msg.not_nil!.body_size = size
      end

      def add_content(bytes)
        raise "No msg to write to" if @next_msg.nil?
        @next_msg.not_nil!.add_content bytes
        send_msg_to_queue if @next_msg.not_nil!.full?
      end

      private def send_msg_to_queue
        ex = @state.exchanges[@next_msg.not_nil!.exchange_name]
        raise "Exchange not declared" if ex.nil?
        queues = ex.queues_matching(@next_msg.not_nil!.routing_key)
        @publish_queues.each do |q|
          q.write_msg(@next_msg.not_nil!)
        end
      end

      def get(queue_name, no_ack)
        q = @state.queues[queue_name]
        raise "Queue #{queue_name} does not exist" if q.nil?
        q.get
      end
    end
  end
end

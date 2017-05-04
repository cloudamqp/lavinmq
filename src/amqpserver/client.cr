require "socket"

module AMQPServer
  class Client
    def initialize(@socket : TCPSocket, @server_state : Server::State)
      @channels = Hash(UInt16, Client::Channel).new
      negotiate_client
    end

    def run_loop
      loop do
        frame = AMQP::Frame.decode @socket
        puts "=> #{frame.inspect}"
        case frame
        when AMQP::Channel::Open
          @channels[frame.channel] = Client::Channel.new(@server_state)
          send AMQP::Channel::OpenOk.new(frame.channel)
        when AMQP::Channel::Close
          @channels.delete frame.channel
          send AMQP::Channel::CloseOk.new(frame.channel)
        when AMQP::Exchange::Declare
          send AMQP::Exchange::DeclareOk.new(frame.channel)
        when AMQP::Connection::Close
          send AMQP::Connection::CloseOk.new
          break
        when AMQP::Basic::Publish
          ex = frame.exchange
          rk = frame.routing_key
          @channels[frame.channel].start_publish(ex, rk)
        when AMQP::HeaderFrame
          @channels[frame.channel].write_body_size(frame.body_size)
        when AMQP::BodyFrame
          @channels[frame.channel].write_content(frame.body)
        end
      end
    ensure
      puts "Client connection closed"
      @socket.close unless @socket.closed?
    end

    def send(frame : AMQP::Frame)
      puts "<= #{frame.inspect}"
      @socket.write frame.to_slice
      @socket.flush
    end

    private def negotiate_client
      start = Bytes.new(8)
      bytes = @socket.read_fully(start)

      if start != AMQP::PROTOCOL_START
        @socket.write AMQP::PROTOCOL_START
        @socket.close
        return
      end

      start = AMQP::Connection::Start.new
      send start
      start_ok = AMQP::Frame.decode @socket
      tune = AMQP::Connection::Tune.new(heartbeat: 0_u16)
      send tune
      tune_ok = AMQP::Frame.decode @socket
      open = AMQP::Frame.decode @socket
      open_ok = AMQP::Connection::OpenOk.new
      send open_ok
    end

    class Channel
      def initialize(@state : Server::State)
        @publish_queues = Array(Server::Queue).new
      end

      def start_publish(exchange_name, routing_key)
        #make sure exchange is declared
        ex = @state.exchanges[exchange_name]
        if ex.nil?
          raise "Exchange not declared"
        end
        case ex[:type]
        when "direct"
          queues = ex[:bindings][routing_key]
          if queues.nil? || queues.empty?
            puts "No queue to publish to"
          else
            queues.each do |q|
              q.write_head(exchange_name, routing_key)
            end
            @publish_queues = queues
          end
        else raise "Exchange type #{ex[:type]} not implemented"
        end
      end

      def write_body_size(size)
        @publish_queues.each do |q|
          q.write_body_size size
        end
      end

      def write_content(bytes)
        @publish_queues.each do |q|
          q.write_content bytes
        end
      end
    end
  end
end

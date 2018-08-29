require "uri"
require "logger"
require "./amqp"
require "./connection"
require "./observable"
require "./client/direct_client"

module AvalancheMQ
  class Upstream
    DEFAULT_PREFETCH        = 1000_u16
    DEFUALT_RECONNECT_DELAY =    1_i32
    DEFAULT_ACK_MODE        = AckMode::OnConfirm
    DEFAULT_MAX_HOPS        = 1
    DEFAULT_EXPIRES         = "none"
    DEFAULT_MSG_TTL         = "none"

    enum AckMode
      OnConfirm
      OnPublish
      NoAck
    end

    @state = 0_u8
    @log : Logger

    getter name, log, vhost, out_ch
    property uri, prefetch, reconnect_delay, ack_mode, trust_user_id

    def initialize(@vhost : VHost, @name : String, raw_uri : String, @prefetch = DEFAULT_PREFETCH,
                   @reconnect_delay = DEFUALT_RECONNECT_DELAY, @ack_mode = DEFAULT_ACK_MODE,
                   @trust_user_id = false)
      @uri = URI.parse(raw_uri)
      @log = @vhost.log.dup
      @log.progname += " upstream=#{@name}"
      @out_ch = Channel::Buffered(AMQP::Frame).new(@prefetch.to_i32)
    end

    def stop
      @state = -1
      @out_ch.close
    end

    class Consumer < Connection
      include Observer

      @client : DirectClient

      def initialize(@upstream : QueueUpstream, @federated_q : Queue)
        @log = @upstream.log.dup
        @log.progname += " consumer"
        @message_counter = 0_u32
        @message_count = 0_u32
        @delivery_tags = Hash(UInt64, UInt64).new
        super(@upstream.uri, @log)
        set_prefetch
        client_properties = {
          "connection_name" => "Federation #{@upstream.name}",
        } of String => AMQP::Field
        @client = @upstream.vhost.direct_client(@upstream.out_ch, client_properties)
        @federated_q.registerObserver(self)
      end

      def on(event, data)
        case event
        when :delete, :close
          force_close
        when :add_consumer
          start
        when :rm_consumer
          force_close unless @federated_q.immediate_delivery?
        end
      end

      def start
        return unless @federated_q.immediate_delivery?
        client_read_loop
        upstream_loop
      end

      private def upstream_loop
        consume
        loop do
          frame = AMQP::Frame.decode(@socket)
          @log.debug { "Read #{frame.inspect}" }
          case frame
          when AMQP::Basic::Deliver
            send_basic_publish(frame)
          when AMQP::HeaderFrame
            @client.write(frame)
          when AMQP::BodyFrame
            @client.write(frame)
          when AMQP::Connection::CloseOk
            break
          when AMQP::Connection::Close
            raise UnexpectedFrame.new(frame) unless @upstream.out_ch.closed?
            @socket.write AMQP::Connection::CloseOk.new.to_slice
            break
          else
            raise UnexpectedFrame.new(frame)
          end
        rescue Channel::ClosedError
          @log.debug { "#upstream_loop out channel closed" }
        end
      ensure
        @socket.close
      end

      private def client_read_loop
        spawn(name: "Upstream consumer #{@upstream.name}#client_read_loop") do
          loop do
            Fiber.yield if @upstream.out_ch.empty?
            frame = @upstream.out_ch.receive
            case frame
            when AMQP::Basic::Ack
              @socket.write frame.to_slice
              @message_counter += 1
            when AMQP::Basic::Nack
              @socket.write frame.to_slice
            when AMQP::Basic::Return
              @socket.write frame.to_slice
            else
              @log.warn { "Unexpected frame #{frame}" }
            end
          rescue ex : Channel::ClosedError
            @log.debug { "#client_read_loop closed" }
            force_close
            break
          end
        end
      end

      private def set_prefetch
        @socket.write AMQP::Basic::Qos.new(1_u16, 0_u32, @upstream.prefetch, false).to_slice
        AMQP::Frame.decode(@socket).as(AMQP::Basic::QosOk)
      end

      private def consume
        queue_name = @upstream.queue.not_nil!
        @socket.write AMQP::Queue::Declare.new(1_u16, 0_u16, queue_name, true,
          false, true, true, false,
          {} of String => AMQP::Field).to_slice
        frame = AMQP::Frame.decode(@socket)
        raise UnexpectedFrame.new(frame) unless frame.is_a?(AMQP::Queue::DeclareOk)
        queue = frame.queue_name
        @message_count = frame.message_count
        no_ack = @upstream.ack_mode == AckMode::NoAck
        consume = AMQP::Basic::Consume.new(1_u16, 0_u16, queue, "downstream_consumer",
          false, no_ack, false, false, {} of String => AMQP::Field)
        @socket.write consume.to_slice
        frame = AMQP::Frame.decode(@socket)
        raise UnexpectedFrame.new(frame) unless frame.is_a?(AMQP::Basic::ConsumeOk)
      end

      def force_close
        return if @socket.closed?
        @socket.write AMQP::Connection::Close.new(320_u16,
          "Federation stopped", 0_u16, 0_u16).to_slice
      end

      def send_basic_publish(frame)
        exchange = ""
        routing_key = @federated_q.name
        mandatory = @upstream.ack_mode == AckMode::OnConfirm
        @client.write AMQP::Basic::Publish.new(1_u16, 0_u16, exchange, routing_key,
          mandatory, false)
        if mandatory
          @message_count += 1
          @delivery_tags[@message_count.to_u64] = frame.delivery_tag
        else
          ack(frame.delivery_tag)
        end
      end

      private def ack(delivery_tag)
        @client.write AMQP::Basic::Ack.new(1_u16, delivery_tag, false)
      end

      private def reject(delivery_tag)
        @client.write AMQP::Basic::Reject.new(1_u16, delivery_tag, false)
      end
    end
  end

  class ExchangeUpstream < Upstream
    property exchange, max_hops, expires, msg_ttl

    @exchange : String?

    def initialize(vhost : VHost, name : String, uri : String, @exchange = nil,
                   @max_hops = DEFAULT_MAX_HOPS, @expires = DEFAULT_EXPIRES,
                   @msg_ttl = DEFAULT_MSG_TTL, prefetch = DEFAULT_PREFETCH,
                   reconnect_delay = DEFUALT_RECONNECT_DELAY, ack_mode = DEFAULT_ACK_MODE,
                   trust_user_id = false)
      super(vhost, name, uri, prefetch.to_u16, reconnect_delay, ack_mode, trust_user_id)
    end

    def link(federated_exchange : Exchange)
    end
  end

  class QueueUpstream < Upstream
    property queue

    @queue : String?

    def initialize(vhost : VHost, name : String, uri : String, @queue = nil,
                   prefetch = DEFAULT_PREFETCH, reconnect_delay = DEFUALT_RECONNECT_DELAY,
                   ack_mode = DEFAULT_ACK_MODE, trust_user_id = false)
      super(vhost, name, uri, prefetch.to_u16, reconnect_delay, ack_mode, trust_user_id)
    end

    def link(federated_q : Queue)
      @state = 0_u8
      @queue ||= federated_q.name
      spawn(name: "Upstream consumer #{@uri.host}/#{@queue}") do
        sub = nil
        loop do
          sub = Consumer.new(self, federated_q)
          @state += 1
          sub.start
        rescue ex
          @state -= 1
          break if @out_ch.closed?
          @log.warn "Upstream consumer failure: #{ex.message}"
          sub.try &.force_close
          sleep @reconnect_delay.seconds
        end
        federated_q.unregisterObserver(sub) unless sub.nil?
        @log.debug { "Consumer stopped" }
      end
      @log.info { "Federation '#{@name}' starting" }
      Fiber.yield
    end
  end
end

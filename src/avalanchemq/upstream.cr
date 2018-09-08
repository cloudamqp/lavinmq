require "uri"
require "logger"
require "./amqp"
require "./connection"
require "./observable"
require "./exchange"
require "./queue"
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

    @log : Logger
    @links = Hash(String, Link).new

    getter name, log, vhost, out_ch, links
    property uri, prefetch, reconnect_delay, ack_mode

    def initialize(@vhost : VHost, @name : String, raw_uri : String, @prefetch = DEFAULT_PREFETCH,
                   @reconnect_delay = DEFUALT_RECONNECT_DELAY, @ack_mode = DEFAULT_ACK_MODE)
      @uri = URI.parse(raw_uri)
      @log = @vhost.log.dup
      @log.progname += " upstream=#{@name}"
    end

    def stop
      @links.values.each(&.close)
      @links.clear
    end

    class Publisher
      @client : DirectClient
      @log : Logger

      def initialize(@upstream : Upstream)
        @log = @upstream.log.dup
        @log.progname += " publisher"
        @out_ch = Channel::Buffered(AMQP::Frame).new(@upstream.prefetch.to_i32)
        client_properties = {
          "connection_name" => "Federation #{@upstream.name}",
        } of String => AMQP::Field
        @client = @upstream.vhost.direct_client(@out_ch, client_properties)
        set_confirm if @upstream.ack_mode == AckMode::OnConfirm
        @message_count = 0_u32
        @delivery_tags = Hash(UInt64, UInt64).new
      end

      def start(@consumer : Consumer)
        client_read_loop
      end

      private def client_read_loop
        spawn(name: "Upstream publisher #{@upstream.name}#client_read_loop") do
          loop do
            Fiber.yield if @out_ch.empty?
            frame = @out_ch.receive
            @log.debug { "Read #{frame.inspect}" }
            case frame
            when AMQP::Basic::Nack
              next unless @upstream.ack_mode == AckMode::OnConfirm
              if frame.multiple
                with_multiple(frame.delivery_tag) { |t| @consumer.not_nil!.reject(t) }
              else
                @consumer.not_nil!.reject(@delivery_tags[frame.delivery_tag])
              end
            when AMQP::Basic::Return
              @consumer.not_nil!.reject(@message_count) if @upstream.ack_mode == AckMode::OnConfirm
            when AMQP::Basic::Ack
              next unless @upstream.ack_mode == AckMode::OnConfirm
              if frame.multiple
                with_multiple(frame.delivery_tag) { |t| @consumer.not_nil!.ack(t) }
              else
                @consumer.not_nil!.ack(@delivery_tags[frame.delivery_tag])
              end
            when AMQP::Connection::CloseOk
              break
            when AMQP::Connection::Close
              @client.write AMQP::Connection::CloseOk.new
              break
            else
              @log.warn { "Unexpected frame #{frame}" }
            end
          rescue ex : Channel::ClosedError
            @log.debug { "#client_read_loop closed" }
            close
            break
          end
        end
      end

      private def set_confirm
        @client.write AMQP::Confirm::Select.new(1_u16, false)
        @out_ch.receive.as(AMQP::Confirm::SelectOk)
      end

      private def with_multiple(delivery_tag)
        @delivery_tags.delete_if do |m, t|
          next false unless m <= delivery_tag
          yield t
          true
        end
      end

      def send_basic_publish(frame, routing_key = nil)
        exchange = ""
        mandatory = @upstream.ack_mode == AckMode::OnConfirm
        pub_frame = AMQP::Basic::Publish.new(frame.channel, 0_u16, exchange, routing_key,
          mandatory, false)
        @log.debug "Send #{pub_frame.inspect}"
        @client.write pub_frame
        if mandatory
          @message_count += 1
          @delivery_tags[@message_count.to_u64] = frame.delivery_tag
        elsif @upstream.ack_mode == AckMode::OnPublish
          @consumer.not_nil!.ack(frame.delivery_tag)
        end
      end

      def write(frame)
        @client.write(frame)
      end

      def close
        return if @client.closed?
        @client.write AMQP::Connection::Close.new(320_u16, "Federation stopped", 0_u16, 0_u16)
      end
    end

    class Consumer < Connection
      def initialize(@upstream : QueueUpstream, @pub : Publisher, @federated_q : Queue)
        @log = @upstream.log.dup
        @log.progname += " consumer"
        super(@upstream.uri, @log)
        set_prefetch
      end

      def start
        return unless @federated_q.immediate_delivery?
        upstream_read_loop
      end

      private def upstream_read_loop
        consume
        loop do
          frame = AMQP::Frame.decode(@socket)
          @log.debug { "Read #{frame.inspect}" }
          case frame
          when AMQP::Basic::Deliver
            @pub.send_basic_publish(frame, @federated_q.name)
          when AMQP::HeaderFrame
            @pub.write(frame)
          when AMQP::BodyFrame
            @pub.write(frame)
          when AMQP::Connection::CloseOk
            break
          when AMQP::Connection::Close
            @socket.write AMQP::Connection::CloseOk.new.to_slice
            raise UnexpectedFrame.new(frame)
          else
            raise UnexpectedFrame.new(frame)
          end
        end
      ensure
        @socket.close
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
        no_ack = @upstream.ack_mode == AckMode::NoAck
        consume = AMQP::Basic::Consume.new(1_u16, 0_u16, queue, "downstream_consumer",
          false, no_ack, false, false, {} of String => AMQP::Field)
        @socket.write consume.to_slice
        frame = AMQP::Frame.decode(@socket)
        raise UnexpectedFrame.new(frame) unless frame.is_a?(AMQP::Basic::ConsumeOk)
      end

      def ack(delivery_tag)
        @socket.write AMQP::Basic::Ack.new(1_u16, delivery_tag, false).to_slice
      end

      def reject(delivery_tag)
        @socket.write AMQP::Basic::Reject.new(1_u16, delivery_tag.to_u64, false).to_slice
      end
    end

    class Link
      include Observer
      getter connected_at

      @publisher : Publisher?
      @consumer : Consumer?

      def initialize(@upstream : QueueUpstream, @federated_q : Queue, @log : Logger)
        @log.progname += " link queue=#{@federated_q.name}:"
        @federated_q.registerObserver(self)
      end

      def on(event, data)
        @log.debug { "event=#{event} data=#{data}" }
        case event
        when :delete, :close
          @upstream.close_link(@federated_q)
        when :rm_consumer
          @upstream.close_link(@federated_q) unless @federated_q.consumer_count > 0
        end
      end

      def start
        @log.debug { "start=#{@federated_q.immediate_delivery?}" }
        return false unless @federated_q.immediate_delivery?
        @consumer.try &.close
        @publisher.try &.close
        @publisher = Publisher.new(@upstream)
        @consumer = Consumer.new(@upstream, @publisher.not_nil!, @federated_q)
        @publisher.not_nil!.start(@consumer.not_nil!)
        @connected_at = Time.utc_now
        @consumer.not_nil!.start
        @log.debug "link stopped"
      end

      def close
        @log.debug "close link"
        @federated_q.unregisterObserver(self)
        @consumer.try &.close
        @publisher.try &.close
      end
    end
  end

  class ExchangeUpstream < Upstream
    property exchange, max_hops, expires, msg_ttl

    @exchange : String?

    def initialize(vhost : VHost, name : String, uri : String, @exchange = nil,
                   @max_hops = DEFAULT_MAX_HOPS, @expires = DEFAULT_EXPIRES,
                   @msg_ttl = DEFAULT_MSG_TTL, prefetch = DEFAULT_PREFETCH,
                   reconnect_delay = DEFUALT_RECONNECT_DELAY, ack_mode = DEFAULT_ACK_MODE)
      super(vhost, name, uri, prefetch.to_u16, reconnect_delay, ack_mode)
    end

    def close_link(federated_exchange : Exchange)
      @links.delete(federated_exchange.name).try(&.close)
      # delete x-federation-upstream exchange on upstream
      # delete queue on upstream
    end

    def link(federated_exchange : Exchange)
      # declare queue on upstream
      # consume queue and publish to downstream exchange
      # declare upstream exchange (passive)
      # declare x-federation-upstream exchange on upstream
      # bind x-federation-upstream exchange to queue
      # get bindings for downstream exchange
      # add bindings from upstream exchange to x-federation-upstream exchange

      # keep downstream exchange bindings reflected on x-federation-upstream exchange
    end
  end

  class QueueUpstream < Upstream
    property queue

    @queue : String?

    def initialize(vhost : VHost, name : String, uri : String, @queue = nil,
                   prefetch = DEFAULT_PREFETCH, reconnect_delay = DEFUALT_RECONNECT_DELAY,
                   ack_mode = DEFAULT_ACK_MODE)
      super(vhost, name, uri, prefetch.to_u16, reconnect_delay, ack_mode)
    end

    def close_link(federated_q : Queue)
      @links.delete(federated_q.name).try(&.close)
    end

    # When federated_q has a consumer the connections are estabished.
    # If all consumers disconnect, the connections are closed.
    # When the policy or the upstream is removed the link is also removed.
    def link(federated_q : Queue)
      @log.debug "link #{federated_q.name}"
      @queue ||= federated_q.name
      link = Link.new(self, federated_q, @log.dup)
      @links[federated_q.name] = link
      spawn(name: "Upstream #{@uri.host}/#{@queue}") do
        sleep 0.05
        loop do
          unless link.start # blocking
            @log.debug { "Waiting for consumers" }
            sleep @reconnect_delay.seconds
          end
        rescue ex
          break unless @links[federated_q.name]?
          @log.warn "Failure: #{ex.inspect_with_backtrace}"
          sleep @reconnect_delay.seconds
        end
        @log.debug { "Link stopped" }
      end
      @log.info { "Link starting" }
      Fiber.yield
    end
  end

  class FederationExchange < TopicExchange
    def type
      "x-federation-upstream"
    end

    def initialize(vhost, name, max_hops = 1)
      arguments = Hash(String, AMQP::Field).new
      arguments["x-internal-purpose"] = "federation"
      arguments["x-max-hops"] = max_hops
      super(vhost, name, durable: true, auto_delete: true, internal: true, arguments: arguments)
    end
  end
end

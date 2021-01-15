require "logger"
require "../sortable_json"
require "amqp-client"
require "http/client"

module AvalancheMQ
  module Shovel
    DEFAULT_ACK_MODE        = AckMode::OnConfirm
    DEFAULT_DELETE_AFTER    = DeleteAfter::Never
    DEFAULT_PREFETCH        = 1000_u16
    DEFAULT_RECONNECT_DELAY =        5

    enum State
      Starting
      Running
      Stopped
      Terminated
    end

    enum DeleteAfter
      Never
      QueueLength
    end

    enum AckMode
      OnConfirm
      OnPublish
      NoAck
    end

    class FailedDeliveryError < Exception; end

    class AMQPSource
      @conn : ::AMQP::Client::Connection?
      @ch : ::AMQP::Client::Channel?
      @q : NamedTuple(queue_name: String, message_count: UInt32, consumer_count: UInt32)?
      @last_unacked : UInt64?

      getter delete_after

      def initialize(@name : String, @uri : URI, @queue : String?, @exchange : String? = nil,
                     @exchange_key : String? = nil,
                     @delete_after = DEFAULT_DELETE_AFTER, @prefetch = DEFAULT_PREFETCH, @ack_mode = DEFAULT_ACK_MODE)
        cfg = Config.instance
        @uri.host ||= "#{cfg.amqp_bind}:#{cfg.amqp_port}"
        unless @uri.user
          direct_user = UserStore.instance.direct_user
          @uri.user = direct_user.name
          @uri.password = direct_user.plain_text_password
        end
        params = @uri.query_params
        params["name"] ||= "Shovel #{@name}"
        @uri.query = params.to_s
        if @queue.nil? && @exchange.nil?
          raise ArgumentError.new("Shovel source requires a queue or an exchange")
        end
      end

      def start
        @conn = conn = ::AMQP::Client.new(@uri).connect
        @ch = ch = conn.channel
        ch.prefetch @prefetch
        q_name = @queue || ""
        @q = q = begin
          ch.queue_declare(q_name, passive: true)
        rescue ::AMQP::Client::Channel::ClosedException
          ch = conn.channel
          ch.queue_declare(q_name, passive: false)
        end
        if @exchange || @exchange_key
          ch.queue_bind(q[:queue_name], @exchange || "", @exchange_key || "")
        end
        "#{@name} #{object_id} source started"
      end

      def stop
        # If we have any outstanding messages when closing, ack them first.
        @ch.try do |ch|
          # Might end up with channel closed
          next if ch.closed?
          @last_unacked.try { |t| ch.basic_ack(t, multiple: true) }
        end
        @conn.try &.close(no_wait: false)
        "#{@name} #{object_id} source stopped"
      end

      def started? : Bool
        !@q.nil?
      end

      def each(&blk : ::AMQP::Client::DeliverMessage -> Nil)
        return unless started?
        q = @q.not_nil!
        ch = @ch.not_nil!
        queue_length = q[:message_count]
        limited = @delete_after == DeleteAfter::QueueLength
        should_ack = @ack_mode != AckMode::NoAck
        return if limited && queue_length.zero?
        tag = "Shovel[#{@name}]"
        ch.basic_consume(q[:queue_name],
          no_ack: !should_ack,
          block: true,
          exclusive: true,
          tag: tag) do |msg|
          blk.call(msg)

          # We batch ack for faster shovel
          batch_full = msg.delivery_tag % (@prefetch / 2).ceil.to_i == 0
          at_end = limited && msg.delivery_tag == queue_length
          if (batch_full || at_end) && should_ack
            msg.ack(multiple: true)
          else
            @last_unacked = msg.delivery_tag
          end

          # Reached end, cancel consumer
          ch.not_nil!.basic_cancel(tag) if at_end
        rescue e : FailedDeliveryError
          msg.reject
        end
      end
    end

    abstract class Destination
      abstract def start

      abstract def stop

      abstract def push(msg)

      abstract def started? : Bool
    end

    class AMQPDestination < Destination
      @conn : ::AMQP::Client::Connection?
      @ch : ::AMQP::Client::Channel?

      def initialize(@name : String, @uri : URI, @queue : String?, @exchange : String? = nil,
                     @exchange_key : String? = nil,
                     @delete_after = DEFAULT_DELETE_AFTER, @prefetch = DEFAULT_PREFETCH, @ack_mode = DEFAULT_ACK_MODE)
        cfg = Config.instance
        @uri.host ||= "#{cfg.amqp_bind}:#{cfg.amqp_port}"
        unless @uri.user
          direct_user = UserStore.instance.direct_user
          @uri.user = direct_user.name
          @uri.password = direct_user.plain_text_password
        end
        params = @uri.query_params
        params["name"] ||= "Shovel #{@name}"
        @uri.query = params.to_s
        if queue
          @exchange = ""
          @exchange_key = queue
        end
        if @exchange.nil?
          raise ArgumentError.new("Shovel destination requires an exchange")
        end
      end

      def start
        conn = ::AMQP::Client.new(@uri).connect
        @conn = conn
        @ch = conn.channel
        "#{@name} #{object_id} destination started"
      end

      def stop
        @conn.try &.close(no_wait: false)
        "#{@name} #{object_id} destination stopped"
      end

      def started? : Bool
        !@ch.nil?
      end

      def push(msg)
        return unless started?
        ch = @ch.not_nil!
        ch.confirm_select if @ack_mode == AckMode::OnConfirm
        msgid = ch.basic_publish(
          msg.body_io,
          @exchange || msg.exchange,
          @exchange_key || msg.routing_key)
        ch.wait_for_confirm(msgid) if @ack_mode == AckMode::OnConfirm
      end
    end

    class HTTPDestination < Destination
      @client : ::HTTP::Client?

      def initialize(@name : String, @uri : URI, @ack_mode = DEFAULT_ACK_MODE)
      end

      def start
        client = ::HTTP::Client.new @uri
        client.connect_timeout = 10
        client.read_timeout = 30
        client.basic_auth(@uri.user, @uri.password || "") if @uri.user
        @client = client
      end

      def stop
        @client.try &.close
      end

      def started? : Bool
        !@client.nil?
      end

      def push(msg)
        return unless started?
        c = @client.not_nil!
        headers = ::HTTP::Headers{"User-Agent" => "AvalancheMQ"}
        headers["X-Shovel"] = @name
        msg.properties.content_type.try { |v| headers["Content-Type"] = v }
        msg.properties.message_id.try { |v| headers["X-Message-Id"] = v }
        msg.properties.headers.try do |hs|
          hs.each do |k, v|
            headers["X-#{k}"] = v.to_s
          end
        end
        path = case
               when !@uri.path.empty?
                 @uri.path
               when p = msg.properties.headers.try { |h| h["uri_path"]? }
                 p.to_s
               else
                 "/"
               end
        response = c.post(path, headers: headers, body: msg.body_io)
        raise FailedDeliveryError.new if @ack_mode == AckMode::OnConfirm && !response.success?
      end
    end

    class Runner
      include SortableJSON
      @log : Logger
      @state = State::Stopped
      @error : String?
      @message_count : UInt64 = 0
      @conn_lock = Mutex.new(:unchecked)

      getter name, vhost

      def initialize(@source : AMQPSource, @destination : Destination,
                     @name : String, @vhost : VHost, @reconnect_delay = DEFAULT_RECONNECT_DELAY)
        @log = @vhost.log.dup
        @log.progname += " shovel=#{@name}"
      end

      def state
        @state.to_s
      end

      def run
        loop do
          break if terminated?
          @state = State::Starting
          @conn_lock.synchronize do
            @source.start unless @source.started?
            @destination.start unless @destination.started?
          end
          @log.info { "started" }
          @state = State::Running
          @source.each do |msg|
            @message_count += 1
            @destination.push(msg)
          end
          @vhost.delete_parameter("shovel", @name) if @source.delete_after == DeleteAfter::QueueLength
          break
        rescue ex : ::AMQP::Client::Connection::ClosedException | ::AMQP::Client::Channel::ClosedException | Socket::ConnectError
          return if terminated?
          @state = State::Stopped
          # Shoveled queue was deleted
          if ex.message.to_s.starts_with?("404")
            break
          end
          @log.error ex.message
          @error = ex.message
          sleep @reconnect_delay.seconds
        rescue ex
          break if terminated?
          @state = State::Stopped
          @log.error ex.inspect_with_backtrace
          @error = ex.message
          sleep @reconnect_delay.seconds
        end
      ensure
        terminate
      end

      def details_tuple
        {
          name:          @name,
          vhost:         @vhost.name,
          state:         @state.to_s,
          error:         @error,
          message_count: @message_count,
        }
      end

      # Does not trigger reconnect, but a graceful close
      def terminate
        return if terminated?
        @state = State::Terminated
        @conn_lock.synchronize do
          @source.stop
          @destination.stop
          @log.info { "terminated" }
        end
      end

      def delete
        terminate
      end

      def terminated?
        @state == State::Terminated
      end

      def running?
        @state == State::Running
      end
    end
  end
end

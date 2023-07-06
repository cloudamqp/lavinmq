require "../sortable_json"
require "amqp-client"
require "http/client"

module LavinMQ
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
      Error
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

      getter delete_after, last_unacked

      def initialize(@name : String, @uri : URI, @queue : String?, @exchange : String? = nil,
                     @exchange_key : String? = nil,
                     @delete_after = DEFAULT_DELETE_AFTER, @prefetch = DEFAULT_PREFETCH, @ack_mode = DEFAULT_ACK_MODE,
                     direct_user : User? = nil)
        @tag = "Shovel[#{@name}]"
        cfg = Config.instance
        @uri.host ||= "#{cfg.amqp_bind}:#{cfg.amqp_port}"
        unless @uri.user
          if direct_user
            @uri.user = direct_user.name
            @uri.password = direct_user.plain_text_password
          else
            raise ArgumentError.new("direct_user required")
          end
        end
        params = @uri.query_params
        params["name"] ||= "Shovel #{@name} source"
        @uri.query = params.to_s
        if @queue.nil? && @exchange.nil?
          raise ArgumentError.new("Shovel source requires a queue or an exchange")
        end
      end

      def start
        if c = @conn
          c.close
        end
        @conn = conn = ::AMQP::Client.new(@uri).connect
        @ch = ch = conn.channel
        ch.prefetch @prefetch
        q_name = @queue || ""
        @q = q = begin
          ch.queue_declare(q_name, passive: true)
        rescue ::AMQP::Client::Channel::ClosedException
          @ch = ch = conn.channel
          ch.queue_declare(q_name, passive: false)
        end
        if @exchange || @exchange_key
          ch.queue_bind(q[:queue_name], @exchange || "", @exchange_key || "")
        end
      end

      def stop
        # If we have any outstanding messages when closing, ack them first.
        @last_unacked.try { |delivery_tag| ack(delivery_tag, close: true) }
        @conn.try &.close(no_wait: false)
      end

      private def at_end?(delivery_tag)
        @q.not_nil![:message_count] == delivery_tag
      end

      def ack(delivery_tag, close = false)
        @ch.try do |ch|
          next if ch.closed?

          # We batch ack for faster shovel
          batch_full = delivery_tag % (@prefetch / 2).ceil.to_i == 0
          if batch_full || at_end?(delivery_tag) || close
            @last_unacked = nil
            ch.basic_ack(delivery_tag, multiple: true)
            if close || (at_end?(delivery_tag) && @delete_after.queue_length?)
              ch.basic_cancel(@tag)
            end
          else
            @last_unacked = delivery_tag
          end
        end
      end

      def started? : Bool
        !@q.nil?
      end

      def each(&blk : ::AMQP::Client::DeliverMessage -> Nil)
        raise "Not started" unless started?
        q = @q.not_nil!
        ch = @ch.not_nil!
        return if @delete_after.queue_length? && q[:message_count].zero?
        ch.basic_consume(q[:queue_name],
          no_ack: @ack_mode.no_ack?,
          exclusive: true,
          block: true,
          tag: @tag) do |msg|
          blk.call(msg)

          if @ack_mode.on_publish?
            ack(msg.delivery_tag)
          end

          if @ack_mode.no_ack? && at_end?(msg.delivery_tag) && @delete_after.queue_length?
            ch.not_nil!.basic_cancel(@tag)
          end
        rescue e : FailedDeliveryError
          msg.reject
        end
      end
    end

    abstract class Destination
      abstract def start

      abstract def stop

      abstract def push(msg, source)

      abstract def started? : Bool
    end

    class AMQPDestination < Destination
      @conn : ::AMQP::Client::Connection?
      @ch : ::AMQP::Client::Channel?

      def initialize(@name : String, @uri : URI, @queue : String?, @exchange : String? = nil,
                     @exchange_key : String? = nil,
                     @delete_after = DEFAULT_DELETE_AFTER, @prefetch = DEFAULT_PREFETCH, @ack_mode = DEFAULT_ACK_MODE,
                     direct_user : User? = nil)
        cfg = Config.instance
        @uri.host ||= "#{cfg.amqp_bind}:#{cfg.amqp_port}"
        unless @uri.user
          if direct_user
            @uri.user = direct_user.name
            @uri.password = direct_user.plain_text_password
          else
            raise ArgumentError.new("direct_user required")
          end
        end
        params = @uri.query_params
        params["name"] ||= "Shovel #{@name} sink"
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
        if c = @conn
          c.close
        end
        conn = ::AMQP::Client.new(@uri).connect
        @conn = conn
        @ch = ch = conn.channel
        if q = @queue
          begin
            ch.queue_declare(q, passive: true)
          rescue ::AMQP::Client::Channel::ClosedException
            @ch = ch = conn.channel
            ch.queue_declare(q, passive: false)
          end
        end
      end

      def stop
        @conn.try &.close
      end

      def started? : Bool
        !@ch.nil?
      end

      def push(msg, source)
        raise "Not started" unless started?
        ch = @ch.not_nil!
        ch.confirm_select if @ack_mode.on_confirm?
        msgid = ch.basic_publish(
          msg.body_io,
          @exchange || msg.exchange,
          @exchange_key || msg.routing_key,
          props: msg.properties)
        if @ack_mode.on_confirm?
          ch.on_confirm(msgid) do
            source.ack(msg.delivery_tag)
          end
        end
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

      def push(msg, source)
        raise "Not started" unless started?
        c = @client.not_nil!
        headers = ::HTTP::Headers{"User-Agent" => "LavinMQ"}
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
               when p = msg.properties.headers.try &.["uri_path"]?
                 p.to_s
               else
                 "/"
               end
        response = c.post(path, headers: headers, body: msg.body_io)
        if @ack_mode.on_confirm?
          raise FailedDeliveryError.new unless response.success?
          source.ack(msg.delivery_tag)
        end
      end
    end

    class Runner
      include SortableJSON
      @log : Log
      @state = State::Stopped
      @error : String?
      @message_count : UInt64 = 0
      @retries : Int64 = 0
      RETRY_THRESHOLD =  10
      MAX_DELAY       = 300

      getter name, vhost

      def initialize(@source : AMQPSource, @destination : Destination,
                     @name : String, @vhost : VHost, @reconnect_delay = DEFAULT_RECONNECT_DELAY)
        @log = @vhost.log.for "shovel=#{@name}"
      end

      def state
        @state.to_s
      end

      def run
        loop do
          break if terminated?
          @state = State::Starting
          unless @source.started?
            if @source.last_unacked
              @log.error { "Restarted with unacked messages, message duplication possible" }
            end
            @source.start
          end
          @destination.start unless @destination.started?
          break if terminated?
          @log.info { "started" }
          @state = State::Running
          @retries = 0
          @source.each do |msg|
            @message_count += 1
            @destination.push(msg, @source)
          end
          @vhost.delete_parameter("shovel", @name) if @source.delete_after.queue_length?
          break
        rescue ex : ::AMQP::Client::Connection::ClosedException | ::AMQP::Client::Channel::ClosedException | Socket::ConnectError
          return if terminated?
          @state = State::Error
          # Shoveled queue was deleted
          if ex.message.to_s.starts_with?("404")
            break
          end
          @log.error(exception: ex) { ex.message }
          @error = ex.message
          exponential_reconnect_delay
        rescue ex
          break if terminated?
          @state = State::Error
          @log.error(exception: ex) { ex.message }
          @error = ex.message
          exponential_reconnect_delay
        end
      ensure
        terminate
      end

      def exponential_reconnect_delay
        @retries += 1
        if @retries > RETRY_THRESHOLD
          sleep Math.min(MAX_DELAY, @reconnect_delay ** (@retries - RETRY_THRESHOLD))
        else
          sleep @reconnect_delay
        end
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
        @state = State::Terminated
        @source.stop
        @destination.stop
        return if terminated?
        @log.info { "terminated" }
      end

      def delete
        terminate
      end

      def terminated?
        @state.terminated?
      end

      def running?
        @state.running?
      end
    end
  end
end

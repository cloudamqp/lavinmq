require "./source"

module LavinMQ
  module Shovel
    class AMQPSource < Source
      Log = LavinMQ::Log.for "amqp_source"
      @conn : ::AMQP::Client::Connection?
      @ch : ::AMQP::Client::Channel?
      @q : NamedTuple(queue_name: String, message_count: UInt32, consumer_count: UInt32)?
      @last_unacked : UInt64?

      getter delete_after, last_unacked

      def initialize(@name : String, @uris : Array(URI), @queue : String?, @exchange : String? = nil,
                     @exchange_key : String? = nil,
                     @delete_after = DEFAULT_DELETE_AFTER, @prefetch = DEFAULT_PREFETCH,
                     @ack_mode = DEFAULT_ACK_MODE, consumer_args : Hash(String, JSON::Any)? = nil,
                     direct_user : Auth::User? = nil, @batch_ack_timeout : Time::Span = DEFAULT_BATCH_ACK_TIMEOUT)
        @tag = "Shovel"
        raise ArgumentError.new("At least one source uri is required") if @uris.empty?
        @uris.each do |uri|
          unless uri.user
            if direct_user
              uri.user = direct_user.name
              uri.password = direct_user.plain_text_password
            else
              raise ArgumentError.new("direct_user required")
            end
          end
          params = uri.query_params
          params["name"] ||= "Shovel #{@name} source"
          uri.query = params.to_s
        end
        if @queue.nil? && @exchange.nil?
          raise ArgumentError.new("Shovel source requires a queue or an exchange")
        end
        @args = AMQ::Protocol::Table.new
        consumer_args.try &.each do |k, v|
          @args[k] = v.as_s?
        end
      end

      def start
        return if started?
        if @last_unacked
          Log.error { "Restarted with unacked messages, message duplication possible" }
        end
        if c = @conn
          c.close
        end
        @conn = ::AMQP::Client.new(@uris.sample).connect
        open_channel
      end

      def stop
        # If we have any outstanding messages when closing, ack them first.
        @ch.try &.basic_cancel(@tag, no_wait: true)
        @last_unacked.try { |delivery_tag| ack(delivery_tag, batch: false) }
        @conn.try &.close(no_wait: false)
        @q = nil
        @ch = nil
      end

      private def at_end?(delivery_tag)
        (q = @q) && @delete_after.queue_length? && q[:message_count] == delivery_tag
      end

      private def past_end?(delivery_tag)
        (q = @q) && @delete_after.queue_length? && q[:message_count] < delivery_tag
      end

      @done = WaitGroup.new(1)

      def ack(delivery_tag, batch = true)
        if ch = @ch
          return if ch.closed?

          # We batch ack for faster shovel
          batch_full = delivery_tag % ack_batch_size == 0
          if !batch || batch_full || at_end?(delivery_tag)
            @last_unacked = nil
            ch.basic_ack(delivery_tag, multiple: true)
            @done.done if at_end?(delivery_tag)
          else
            @last_unacked = delivery_tag
          end
        end
      end

      def started? : Bool
        !@q.nil? && !@conn.try &.closed?
      end

      private def ack_batch_size
        (@prefetch / 2).ceil.to_i
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def open_channel
        @ch.try &.close
        conn = @conn || raise "Connection not established"
        @ch = ch = conn.channel
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
        if @delete_after.queue_length? && q[:message_count] > 0
          @prefetch = Math.min(q[:message_count], @prefetch).to_u16
        end
        ch.prefetch @prefetch

        # We only start timeout loop if we're actually batching
        if ack_batch_size > 1
          spawn(name: "Shovel #{@name} ack timeout loop") { ack_timeout_loop(ch) }
        end
      end

      private def ack_timeout_loop(ch)
        batch_ack_timeout = @batch_ack_timeout
        Log.trace { "ack_timeout_loop starting for ch #{ch}" }
        loop do
          last_unacked = @last_unacked
          sleep batch_ack_timeout

          break if ch.closed?

          # We have nothing in memory
          next if last_unacked.nil?

          # @last_unacked is nil after an ack has been sent, i.e if nil
          # there is nothing to ack
          next if @last_unacked.nil?

          # Our memory is the same as the current @last_unacked which means
          # that nothing has happend, lets ack!
          if last_unacked == @last_unacked
            ack(last_unacked, batch: false)
          end
        end
        Log.trace { "ack_timeout_loop stopped for ch #{ch}" }
      end

      def each(&blk : ::AMQP::Client::DeliverMessage -> Nil)
        q = @q || raise "Not started"
        ch = @ch || raise "Not started"
        exclusive = !@args["x-stream-offset"]? # consumers for streams can not be exclusive
        return if @delete_after.queue_length? && q[:message_count].zero?
        ch.basic_consume(q[:queue_name],
          no_ack: @ack_mode.no_ack?,
          exclusive: exclusive,
          block: true,
          args: @args,
          tag: @tag) do |msg|
          blk.call(msg) unless past_end?(msg.delivery_tag)
          if at_end?(msg.delivery_tag)
            ch.basic_cancel(@tag, no_wait: true)
            @done.wait # wait for last ack before returning, which will close connection
          end
        rescue e : FailedDeliveryError
          msg.reject
        end
      rescue e
        Log.warn { "name=#{@name} #{e.message}" }
        stop
        raise e
      end
    end
  end
end

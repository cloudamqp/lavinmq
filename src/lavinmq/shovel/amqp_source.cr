require "amqp-client"
require "./source"

module LavinMQ
  module Shovel
    class AMQPSource < Source
      Log = LavinMQ::Log.for "amqp_source"
      @conn : ::AMQP::Client::Connection?
      @ch : ::AMQP::Client::Channel?
      @q : NamedTuple(queue_name: String, message_count: UInt32, consumer_count: UInt32)?
      @last_unacked : UInt64?
      # Queue-length mode: how many of the messages that were on the queue at
      # start (the message_count snapshot) have been settled for good — acked,
      # or rejected without requeue.
      @settled = 0_u32

      getter delete_after, last_unacked

      def initialize(@name : String, @uris : Array(URI), @queue : String?, @exchange : String? = nil,
                     @exchange_key : String? = nil,
                     @delete_after = DEFAULT_DELETE_AFTER, @prefetch = DEFAULT_PREFETCH,
                     @ack_mode = DEFAULT_ACK_MODE, consumer_args : Hash(String, JSON::Any)? = nil,
                     direct_user : Auth::User? = nil, @batch_ack_timeout : Time::Span = DEFAULT_BATCH_ACK_TIMEOUT)
        @tag = "Shovel"
        raise ArgumentError.new("At least one source uri is required") if @uris.empty?
        @uris.each do |uri|
          if uri.user.nil? && uri.host.to_s.empty?
            # unless uri.user
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
        @settle.synchronize do
          if (ch = @ch) && !ch.closed? && (tag = @last_unacked)
            flush_ack(ch, tag)
          end
        end
        @conn.try &.close(no_wait: false)
        @q = nil
        @ch = nil
      end

      # Queue-length mode moves the messages that were on the queue at start
      # and no more. A delivery tag past the snapshot is a newer message —
      # unless it is a redelivery of one of ours that was requeued (Retry,
      # Abort), which still has to be moved.
      private def past_end?(msg : ::AMQP::Client::DeliverMessage) : Bool
        return false unless (q = @q) && @delete_after.queue_length?
        msg.delivery_tag > q[:message_count] && !msg.redelivered
      end

      # Records one message settled for good. Returns true when it was the last
      # of the snapshot, i.e. the queue-length run is complete.
      private def settle_one : Bool
        return false unless (q = @q) && @delete_after.queue_length?
        @settled += 1
        @settled >= q[:message_count]
      end

      # Every message of the snapshot is settled: stop consuming. Cancelling
      # closes the consumer's delivery channel, so the blocking consume in #each
      # returns and the Runner finishes the shovel. Any final ack was written
      # before the cancel, so it is on the wire first.
      private def finish(ch)
        ch.basic_cancel(@tag, no_wait: true)
      end

      # Serializes settlement (ack/reject/timeout-flush/stop). @last_unacked is
      # written from the confirm fiber and the ack-timeout fiber, which run on
      # separate threads under -Dpreview_mt; the read-decide-emit-update must be
      # indivisible, or a flush could double-settle a tag.
      @settle = Mutex.new

      def ack(delivery_tag, batch = true)
        @settle.synchronize { ack_locked(delivery_tag, batch) }
      end

      private def ack_locked(delivery_tag, batch)
        ch = @ch
        return unless ch
        return if ch.closed?

        final = settle_one
        # We batch ack for faster shovel
        batch_full = delivery_tag % ack_batch_size == 0
        if !batch || batch_full || final
          flush_ack(ch, delivery_tag)
          finish(ch) if final
        else
          @last_unacked = delivery_tag
        end
      end

      # Ack `delivery_tag` and everything deferred before it. A flush, not a
      # settlement: the deferred tag was counted when its ack came in.
      private def flush_ack(ch, delivery_tag)
        @last_unacked = nil
        ch.basic_ack(delivery_tag, multiple: true)
      end

      # Return a single message to the source. We ack with multiple: true for
      # throughput, so before rejecting tag T we must flush any pending batched
      # ack of earlier tags — otherwise a later multiple-ack would settle T too.
      def reject(delivery_tag, requeue)
        @settle.synchronize { reject_locked(delivery_tag, requeue) }
      end

      private def reject_locked(delivery_tag, requeue)
        ch = @ch
        return unless ch
        return if ch.closed?
        if last = @last_unacked
          flush_ack(ch, last) if last < delivery_tag
        end
        ch.basic_reject(delivery_tag, requeue: requeue)
        # A requeued message comes back redelivered and is settled then; a
        # dead-lettered (or dropped) one is settled now.
        finish(ch) if !requeue && settle_one
      end

      def started? : Bool
        !@q.nil? && !@conn.try &.closed?
      end

      private def ack_batch_size
        (@prefetch / 2).ceil.to_i
      end

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

          # Re-check and flush under the settlement lock so a concurrent
          # ack/reject can't change @last_unacked between the check and the
          # flush. If it has moved on (or been settled), there's nothing to do.
          @settle.synchronize do
            flush_ack(ch, last_unacked) if !ch.closed? && last_unacked == @last_unacked
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
          blk.call(msg) unless past_end?(msg)
          # no-ack settles nothing, so with nothing to requeue the snapshot is
          # complete once its last message has been delivered.
          finish(ch) if @ack_mode.no_ack? && @delete_after.queue_length? && msg.delivery_tag == q[:message_count]
        end
      rescue e
        Log.warn { "name=#{@name} #{e.message}" }
        stop
        raise e
      end
    end
  end
end

require "amqp-client"
require "./source"

module LavinMQ
  module Shovel
    class AMQPSource < Source
      Log = LavinMQ::Log.for "amqp_source"
      @conn : ::AMQP::Client::Connection?
      @ch : ::AMQP::Client::Channel?
      @q : NamedTuple(queue_name: String, message_count: UInt32, consumer_count: UInt32)?

      # Settlement bookkeeping, all guarded by @settle. Delivery tags on a
      # channel are consecutive, so "everything up to here is settled" is one
      # number: @frontier is the highest tag such that every tag at or below it
      # has been acked or rejected, @flushed the highest tag acked to the broker
      # (cumulatively). A tag settled out of order — a destination that confirms
      # 3 before 2 — waits in @settled_above until the gap below it closes; a
      # cumulative ack must never cover a tag whose delivery is still pending.
      @frontier = 0_u64
      @flushed = 0_u64
      @settled_above = Set(UInt64).new
      # Messages handed to the Runner and not yet settled either way, and the
      # total handed over, for the drain check in queue-length mode.
      @in_flight = 0_u32
      @deliveries = 0_u64
      # Queue-length mode: how many messages have been settled for good (acked,
      # or rejected without requeue) against the message_count snapshot.
      @settled = 0_u32

      getter delete_after

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
        if pending_ack
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
          if (ch = @ch) && !ch.closed?
            flush_ack(ch)
          end
        end
        @conn.try &.close(no_wait: false)
        @q = nil
        @ch = nil
      end

      # The highest settled tag not yet acked to the broker, if any.
      def pending_ack : UInt64?
        @frontier if @frontier > @flushed
      end

      # Records one message settled for good. Returns true when it was the last
      # of the snapshot, i.e. the queue-length run is complete.
      private def settle_one : Bool
        return false unless (q = @q) && @delete_after.queue_length?
        @settled += 1
        @settled >= q[:message_count]
      end

      # The queue-length run is complete: stop consuming. Cancelling closes the
      # consumer's delivery channel, so the blocking consume in #each returns
      # and the Runner finishes the shovel. Any final ack was written before
      # the cancel, so it is on the wire first.
      private def finish(ch)
        ch.basic_cancel(@tag, no_wait: true)
      end

      # Serializes settlement (ack/reject/timeout-flush/stop). The frontier is
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
        settle_tag(delivery_tag)
        final = settle_one
        # We batch ack for faster shovel
        if !batch || @frontier - @flushed >= ack_batch_size || final
          flush_ack(ch)
          finish(ch) if final
        end
      end

      # Marks `delivery_tag` settled at the broker (acked or rejected) and moves
      # the frontier over it, and over any tags settled earlier that were
      # waiting for it.
      private def settle_tag(delivery_tag)
        @in_flight -= 1 unless @in_flight.zero?
        if delivery_tag == @frontier + 1
          @frontier = delivery_tag
          while @settled_above.delete(@frontier + 1)
            @frontier += 1
          end
        elsif delivery_tag > @frontier
          @settled_above << delivery_tag
        end
      end

      # Ack everything settled so far in one cumulative ack. A flush, not a
      # settlement: each tag was counted when its ack or reject came in.
      private def flush_ack(ch)
        return if @frontier <= @flushed
        ch.basic_ack(@frontier, multiple: true)
        @flushed = @frontier
      end

      # Return a single message to the source. A reject settles its tag at the
      # broker, so the frontier moves over it and a later cumulative ack is
      # free to pass it.
      def reject(delivery_tag, requeue)
        @settle.synchronize { reject_locked(delivery_tag, requeue) }
      end

      private def reject_locked(delivery_tag, requeue)
        ch = @ch
        return unless ch
        return if ch.closed?
        ch.basic_reject(delivery_tag, requeue: requeue)
        settle_tag(delivery_tag)
        if requeue
          # A requeued message comes back redelivered and is settled then —
          # unless the broker dropped it (delivery limit, TTL) instead, in
          # which case nothing is left to arrive and the run must not wait.
          schedule_drain_check(ch) if @in_flight.zero? && @delete_after.queue_length?
        elsif settle_one
          # A dead-lettered (or dropped) message is settled now.
          flush_ack(ch)
          finish(ch)
        end
      end

      # With nothing in flight after a requeue, look at the queue once the
      # redelivery has had time to arrive: if it did, deliveries moved on; if
      # the queue is empty instead, the message is gone and the run is done.
      private def schedule_drain_check(ch)
        seen = @deliveries
        spawn(name: "Shovel #{@name} drain check") do
          sleep @batch_ack_timeout
          @settle.synchronize do
            next if ch.closed? || @deliveries != seen || !@in_flight.zero?
            finish(ch) if queue_drained?(ch)
          end
        end
      end

      private def queue_drained?(ch) : Bool
        q = @q || return false
        ch.queue_declare(q[:queue_name], passive: true)[:message_count].zero?
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
        q = begin
          ch.queue_declare(q_name, passive: true)
        rescue ::AMQP::Client::Channel::ClosedException
          @ch = ch = conn.channel
          ch.queue_declare(q_name, passive: false)
        end
        # A new channel numbers its deliveries from 1 again, and a queue-length
        # run counts against the snapshot just taken, not the previous one's.
        @settle.synchronize do
          @q = q
          @frontier = @flushed = 0_u64
          @settled_above.clear
          @in_flight = 0_u32
          @settled = 0_u32
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

      # Flush a batch that has been waiting a whole timeout without growing.
      private def ack_timeout_loop(ch)
        batch_ack_timeout = @batch_ack_timeout
        Log.trace { "ack_timeout_loop starting for ch #{ch}" }
        loop do
          pending = pending_ack
          sleep batch_ack_timeout

          break if ch.closed?

          # We have nothing in memory
          next if pending.nil?

          # Re-check and flush under the settlement lock so a concurrent
          # ack/reject can't move the frontier between the check and the flush.
          # If it has moved on (or been flushed), there's nothing to do.
          @settle.synchronize do
            flush_ack(ch) if !ch.closed? && pending == pending_ack
          end
        end
        Log.trace { "ack_timeout_loop stopped for ch #{ch}" }
      end

      # Queue-length mode moves as many messages as were on the queue at start
      # (the message_count snapshot) and then finishes. Requeued messages come
      # back and count when settled; a message published after the start may
      # be delivered into a slot a settled one freed and is moved like any
      # other — nothing delivered is ever left unacked.
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
          @settle.synchronize do
            @deliveries += 1
            @in_flight += 1
          end
          blk.call(msg)
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

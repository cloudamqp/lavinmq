require "./source"
require "../endpoint/session"

module LavinMQ
  module Shovel
    # Consumes a queue, or an exchange through a queue bound to it, on an
    # Endpoint: this broker in-process or another broker over AMQP. Every
    # start picks one of the sessions (one per `src-uri`) at random.
    class AMQPSource < Source
      Log = LavinMQ::Log.for "shovel.amqp_source"
      TAG = "Shovel"

      @session : Endpoint::Session?
      # Name and message count of the consumed queue at start
      @q : Tuple(String, UInt32)?

      # Settlement bookkeeping, all guarded by @settle. Delivery tags in a
      # session are consecutive, so "everything up to here is settled" is one
      # number: @frontier is the highest tag such that every tag at or below it
      # has been acked or rejected, @flushed the highest tag acked to the broker
      # (cumulatively). A tag settled out of order — a destination that confirms
      # 3 before 2 — waits in @settled_above until the gap below it closes; a
      # cumulative ack must never cover a tag whose delivery is still pending.
      #
      # A reject settles its tag at the broker by itself, so the cumulative ack
      # names @ack_frontier, the highest *acked* tag at or below @frontier,
      # never a rejected one: the broker only accepts a cumulative ack for a tag
      # it still holds, and answers 406 "unknown delivery tag" — closing the
      # channel — for one it has already settled. Acks settled out of order
      # wait in @acked_above until the frontier reaches them.
      @frontier = 0_u64
      @ack_frontier = 0_u64
      @flushed = 0_u64
      @settled_above = Set(UInt64).new
      @acked_above = Set(UInt64).new
      # Messages handed to the Runner and not yet settled either way, and the
      # total handed over, for the drain check in queue-length mode.
      @in_flight = 0_u32
      @deliveries = 0_u64
      # Queue-length mode: how many messages have been settled for good (acked,
      # or rejected without requeue) against the message_count snapshot.
      @settled = 0_u32
      # Serializes settlement (ack/reject/timeout-flush/stop). The frontier is
      # written from the confirm fiber and the ack-timeout fiber, which run on
      # separate threads under -Dpreview_mt; the read-decide-emit-update must be
      # indivisible, or a flush could double-settle a tag.
      @settle = Mutex.new

      getter delete_after

      def initialize(@name : String, @sessions : Array(Endpoint::Session), @queue : String?,
                     @exchange : String? = nil, @exchange_key : String? = nil,
                     @delete_after = DEFAULT_DELETE_AFTER, @prefetch = DEFAULT_PREFETCH,
                     @ack_mode = DEFAULT_ACK_MODE, @consumer_args = AMQ::Protocol::Table.new,
                     @batch_ack_timeout : Time::Span = DEFAULT_BATCH_ACK_TIMEOUT)
        raise ArgumentError.new("At least one source uri is required") if @sessions.empty?
        if @queue.nil? && @exchange.nil?
          raise ArgumentError.new("Shovel source requires a queue or an exchange")
        end
      end

      def start
        return if started?
        if pending_ack
          Log.error { "Restarted with unacked messages, message duplication possible" }
        end
        @session.try &.close
        session = @sessions.sample
        session.open
        @session = session
        begin
          open_queue(session)
        rescue ex
          session.close
          raise ex
        end
      end

      def stop
        session = @session || return
        session.cancel(TAG)
        # Acks that are settled but not yet flushed are flushed before closing,
        # so the close only returns what's really unsettled.
        @settle.synchronize { flush_ack(session) unless session.closed? }
        stop_ack_timer
        session.close
        @q = nil
      end

      def started? : Bool
        return false if @q.nil?
        session = @session
        !session.nil? && !session.closed?
      end

      # The highest acked tag not yet acked to the broker, if any.
      def pending_ack : UInt64?
        @ack_frontier if @ack_frontier > @flushed
      end

      def ack(delivery_tag, batch = true)
        @settle.synchronize { ack_locked(delivery_tag, batch) }
      end

      private def ack_locked(delivery_tag, batch)
        session = @session || return
        return if session.closed?
        settle_tag(delivery_tag, acked: true)
        final = settle_one
        # A full batch is flushed, and so is a partial one once nothing is in
        # flight: no settlement is coming to grow it. Under load deliveries
        # are always in flight, so acks go out once per batch.
        if !batch || final || @in_flight.zero? || @frontier - @flushed >= ack_batch_size
          flush_ack(session)
          finish(session) if final
        elsif pending_ack
          arm_ack_timer
        end
      end

      # Return a single message to the source. A reject settles its tag at the
      # broker, so the frontier moves over it and a later cumulative ack is
      # free to pass it.
      def reject(delivery_tag, requeue)
        @settle.synchronize { reject_locked(delivery_tag, requeue) }
      end

      private def reject_locked(delivery_tag, requeue)
        session = @session || return
        return if session.closed?
        session.reject(delivery_tag, requeue: requeue)
        settle_tag(delivery_tag)
        flush_ack(session) if @in_flight.zero?
        if requeue
          # A requeued message comes back redelivered and is settled then —
          # unless the broker dropped it (delivery limit, TTL) instead, in
          # which case nothing is left to arrive and the run must not wait.
          schedule_drain_check(session) if @in_flight.zero? && @delete_after.queue_length?
        elsif settle_one
          # A dead-lettered (or dropped) message is settled now.
          flush_ack(session)
          finish(session)
        end
      end

      # Records one message settled for good. Returns true when it was the last
      # of the snapshot, i.e. the queue-length run is complete.
      private def settle_one : Bool
        return false unless (q = @q) && @delete_after.queue_length?
        @settled += 1
        @settled >= q[1]
      end

      # The queue-length run is complete: stop consuming. Cancelling makes the
      # blocking consume in #each return, and the Runner finishes the shovel.
      # Any final ack was sent before the cancel.
      private def finish(session)
        session.cancel(TAG)
      end

      # Marks `delivery_tag` settled (acked or rejected) and moves the frontier
      # over it, and over any tags settled earlier that were waiting for it.
      # The ack frontier follows, stopping at the highest acked tag.
      private def settle_tag(delivery_tag, acked = false)
        @in_flight -= 1 unless @in_flight.zero?
        if delivery_tag == @frontier + 1
          @frontier = delivery_tag
          @ack_frontier = delivery_tag if acked
          while @settled_above.delete(@frontier + 1)
            @frontier += 1
            @ack_frontier = @frontier if @acked_above.delete(@frontier)
          end
        elsif delivery_tag > @frontier
          @settled_above << delivery_tag
          @acked_above << delivery_tag if acked
        end
      end

      # Ack everything acked so far in one cumulative ack. A flush, not a
      # settlement: each tag was counted when its ack or reject came in.
      private def flush_ack(session)
        tag = pending_ack || return
        session.ack(tag, multiple: true)
        @flushed = tag
      end

      # With nothing in flight after a requeue, look at the queue once the
      # redelivery has had time to arrive: if it did, deliveries moved on; if
      # the queue is empty instead, the message is gone and the run is done.
      private def schedule_drain_check(session)
        seen = @deliveries
        spawn(name: "Shovel #{@name} drain check") do
          sleep @batch_ack_timeout
          @settle.synchronize do
            next if session.closed? || @deliveries != seen || !@in_flight.zero?
            finish(session) if queue_drained?(session)
          end
        end
      end

      private def queue_drained?(session) : Bool
        q = @q || return false
        session.declare_queue(q[0], passive: true)[1].zero?
      rescue Endpoint::Error
        false
      end

      # Acks to an AMQP broker are batched, one cumulative ack per half
      # prefetch. In-process there's nothing to save, so every ack is sent
      # right away and the queue's unacked count stays accurate.
      private def ack_batch_size
        return 1 if @session.is_a?(Endpoint::LocalSession)
        (@prefetch / 2).ceil.to_i
      end

      private def open_queue(session)
        stop_ack_timer # of a previous run
        q_name = @queue || ""
        q = begin
          session.declare_queue(q_name, passive: true)
        rescue Endpoint::NotFound
          # A server named queue for an exchange source is transient
          session.declare_queue(q_name, passive: false, durable: !q_name.empty?,
            auto_delete: q_name.empty?)
        end
        # A new session numbers its deliveries from 1 again, and a queue-length
        # run counts against the snapshot just taken, not the previous one's.
        @settle.synchronize do
          @q = q
          @frontier = @ack_frontier = @flushed = 0_u64
          @settled_above.clear
          @acked_above.clear
          @in_flight = 0_u32
          @settled = 0_u32
        end
        if @exchange || @exchange_key
          session.bind_queue(q[0], @exchange || "", @exchange_key || "")
        end
        prefetch = @prefetch
        if @delete_after.queue_length? && q[1] > 0
          prefetch = Math.min(q[1], prefetch).to_u16
        end
        session.prefetch = prefetch
        # Only batched acks need a deadline for a partial batch
        if ack_batch_size > 1
          arm = ::Channel(Bool).new(1)
          done = ::Channel(Nil).new
          @ack_timer = {arm, done}
          spawn(name: "Shovel #{@name} ack timer") { ack_timer(session, arm, done) }
        end
      end

      # Armed when an ack is left pending in a partial batch, closed by #stop
      @ack_timer : Tuple(::Channel(Bool), ::Channel(Nil))?

      private def arm_ack_timer
        @ack_timer.try &.[0].try_send?(true)
      end

      # Flushes a partial batch at most batch_ack_timeout after it was armed:
      # deliveries are still in flight (else the ack would have been flushed
      # already) but they may take long, e.g. a destination being retried.
      # Sleeps on the arm channel while there's nothing pending, no polling.
      private def ack_timer(session, arm, done)
        while arm.receive? # Bool, as receive? of nil can't be told from closed
          select
          when done.receive?
            return
          when timeout(@batch_ack_timeout)
          end
          @settle.synchronize { flush_ack(session) unless session.closed? }
        end
      end

      private def stop_ack_timer
        if timer = @ack_timer
          timer[0].close
          timer[1].close
          @ack_timer = nil
        end
      end

      # Queue-length mode moves as many messages as were on the queue at start
      # (the message_count snapshot) and then finishes. Requeued messages come
      # back and count when settled; a message published after the start may
      # be delivered into a slot a settled one freed and is moved like any
      # other — nothing delivered is ever left unacked.
      def each(&blk : Endpoint::Delivery -> Nil)
        q = @q || raise "Not started"
        session = @session || raise "Not started"
        return if @delete_after.queue_length? && q[1].zero?
        # Stream consumers can't be exclusive
        exclusive = !@consumer_args.has_key?("x-stream-offset")
        no_ack = @ack_mode.no_ack?
        session.consume(q[0], TAG, no_ack, exclusive, @consumer_args) do |msg|
          @settle.synchronize do
            @deliveries += 1
            @in_flight += 1
          end
          blk.call(msg)
          # no-ack settles nothing, so with nothing to requeue the snapshot is
          # complete once its last message has been delivered.
          finish(session) if no_ack && @delete_after.queue_length? && msg.tag == q[1]
        end
      rescue ex
        Log.warn { "name=#{@name} #{ex.message}" }
        stop
        raise ex
      end
    end
  end
end

require "../sortable_json"
require "./constants"
require "./destination"
require "./source"

module LavinMQ
  module Shovel
    class Runner
      include SortableJSON
      @state = State::Stopped
      @error : String?
      @message_count : UInt64 = 0
      @retries : Int64 = 0
      @stop_generation = Atomic(UInt64).new(0_u64)
      # Consecutive transient (Retry) delivery failures, written by the outcome
      # handler (confirm fiber) and read by the consuming loop for backoff.
      @delivery_failures = Atomic(Int32).new(0)
      # Consecutive Abort outcomes; past ABORT_THRESHOLD the shovel errors out.
      @delivery_aborts = Atomic(Int32).new(0)
      @aborted = false
      # Cumulative per-disposition counters, for the runtime view.
      @confirmed_total = Atomic(UInt64).new(0_u64)
      @retried_total = Atomic(UInt64).new(0_u64)
      @rejected_total = Atomic(UInt64).new(0_u64)
      @aborted_total = Atomic(UInt64).new(0_u64)
      RETRY_THRESHOLD    =  10
      ABORT_THRESHOLD    =  10
      MAX_DELAY          = 300
      MAX_DELIVERY_DELAY =  30

      getter name, vhost

      def initialize(@source : Source, @destination : Destination,
                     @name : String, @vhost : VHost, @reconnect_delay : Time::Span = DEFAULT_RECONNECT_DELAY)
        filename = "shovels.#{Digest::SHA1.hexdigest @name}.paused"
        @paused_file_path = File.join(Config.instance.data_dir, filename)
        if File.exists?(@paused_file_path)
          @state = State::Paused
        end
      end

      def state
        @state
      end

      def run
        Log.context.set(name: @name, vhost: @vhost.name)
        run_generation = @stop_generation.get(:acquire)
        register_outcome_handler
        loop do
          break if should_stop_loop?(run_generation)
          @state = State::Starting
          @source.start
          @destination.start

          break if should_stop_loop?(run_generation)
          Log.info { "started" }
          @state = State::Running
          @retries = 0
          @source.each do |msg|
            @message_count += 1
            # Paused/terminated: start no new delivery. An already in-flight push
            # can't be interrupted, but we don't begin another one.
            next if should_stop_loop?(run_generation)
            check_abort_threshold
            backoff_if_failing
            @destination.push(msg)
          end
          break if should_stop_loop?(run_generation) # Don't delete shovel if paused/terminated
          @vhost.delete_parameter("shovel", @name) if @source.delete_after.queue_length?
          break
        rescue ex : ShovelAborted
          error_out(ex)
          break
        rescue ex
          break if handle_run_error(ex, run_generation)
        end
      ensure
        terminate_if_needed(run_generation)
      end

      # The single place a delivery Outcome becomes a source action: a confirmed
      # delivery acks; a rejection (broker nack or HTTP error) requeues so
      # queue-based retry/DLX policies still apply. Registered once per run.
      private def register_outcome_handler
        source = @source
        @destination.on_outcome = ->(delivery_tag : UInt64, outcome : Outcome) do
          case outcome
          in Outcome::Confirmed
            @confirmed_total.add(1)
            @delivery_failures.set(0)
            @delivery_aborts.set(0)
            source.ack(delivery_tag)
          in Outcome::Retry
            # A non-abort outcome breaks any consecutive-abort streak: the
            # destination is reachable, just not accepting this message yet.
            @retried_total.add(1)
            @delivery_aborts.set(0)
            @delivery_failures.add(1)
            source.reject(delivery_tag, requeue: true)
          in Outcome::Reject
            # The endpoint responded (it just refused this message), so it is
            # healthy — clear the backoff and the abort streak.
            @rejected_total.add(1)
            @delivery_failures.set(0)
            @delivery_aborts.set(0)
            source.reject(delivery_tag, requeue: false)
          in Outcome::Abort
            # Keep the message; the consuming loop errors-out the shovel once
            # consecutive Aborts cross ABORT_THRESHOLD.
            @aborted_total.add(1)
            @delivery_aborts.add(1)
            source.reject(delivery_tag, requeue: true)
          end
          nil
        end
      end

      # Sleep before the next delivery in proportion to recent transient
      # failures, so a persistently-rejecting destination is retried with
      # capped exponential backoff rather than in a tight loop.
      private def backoff_if_failing
        failures = @delivery_failures.get
        return if failures.zero?
        sleep self.class.delivery_backoff(failures)
      end

      # Backoff before the next delivery attempt after `failures` consecutive
      # transient failures: 0.5s, 1s, 2s, 4s, … doubling, capped at
      # MAX_DELIVERY_DELAY, then holding there.
      def self.delivery_backoff(failures : Int32) : Time::Span
        return 0.seconds if failures <= 0
        secs = Math.min(MAX_DELIVERY_DELAY.to_f, 0.5 * 2.0 ** Math.min(failures - 1, 6))
        secs.seconds
      end

      # Errors-out the shovel once a destination has been classified unusable
      # (Abort) too many times in a row. Raised inside the consuming loop.
      private def check_abort_threshold
        return if @delivery_aborts.get < ABORT_THRESHOLD
        raise ShovelAborted.new("destination unusable after #{ABORT_THRESHOLD} attempts")
      end

      # Terminal: the destination is unusable. Stay in Error for the operator
      # rather than reconnecting.
      private def error_out(ex)
        @aborted = true
        @state = State::Error
        @error = ex.message
        Log.warn { "Aborted: #{ex.message}" }
        @source.stop
        @destination.stop
      end

      # Handles a connection/runtime error during a run. Returns true if the run
      # loop should break (stopped, or the shoveled queue was deleted), false to
      # reconnect with backoff.
      private def handle_run_error(ex, run_generation) : Bool
        return true if should_stop_loop?(run_generation)
        @state = State::Error
        return true if ex.message.to_s.starts_with?("404") # shoveled queue was deleted
        Log.warn { ex.message }
        @error = ex.message
        exponential_reconnect_delay
        false
      end

      private def terminate_if_needed(run_generation)
        return if @aborted # keep the Error state for the operator
        return if stopped_by_newer_generation?(run_generation)
        terminate if !paused?
      end

      def exponential_reconnect_delay
        @retries += 1
        if @retries > RETRY_THRESHOLD
          sleep Math.min(MAX_DELAY, @reconnect_delay.seconds ** (@retries - RETRY_THRESHOLD)).seconds
        else
          sleep @reconnect_delay
        end
      end

      def details_tuple
        {
          name:                 @name,
          vhost:                @vhost.name,
          state:                @state.to_s,
          error:                @error,
          message_count:        @message_count,
          confirmed:            @confirmed_total.get,
          retried:              @retried_total.get,
          dead_lettered:        @rejected_total.get,
          aborted:              @aborted_total.get,
          consecutive_failures: @delivery_failures.get,
          consecutive_aborts:   @delivery_aborts.get,
          abort_threshold:      ABORT_THRESHOLD,
        }
      end

      def resume
        return unless paused?
        delete_paused_file
        @state = State::Starting
        Log.info { "Resuming shovel #{@name} vhost=#{@vhost.name}" }
        spawn(run, name: "Shovel name=#{@name} vhost=#{@vhost.name}")
      end

      def pause
        return if terminated?
        File.write(@paused_file_path, @name)
        Log.info { "Pausing shovel #{@name} vhost=#{@vhost.name}" }
        @state = State::Paused
        @stop_generation.add(1_u64, :release)
        @source.stop
        @destination.stop
        Log.info &.emit("Paused", name: @name, vhost: @vhost.name)
      end

      def delete
        terminate
        delete_paused_file
      end

      # Does not trigger reconnect, but a graceful close
      def terminate
        @state = State::Terminated
        @stop_generation.add(1_u64, :release)
        @source.stop
        @destination.stop
        return if terminated?
        Log.info &.emit("Terminated", name: @name, vhost: @vhost.name)
      end

      def delete_paused_file
        FileUtils.rm(@paused_file_path) if File.exists?(@paused_file_path)
      end

      def should_stop_loop?(run_generation)
        stopped_by_newer_generation?(run_generation) || terminated? || paused?
      end

      private def stopped_by_newer_generation?(run_generation) : Bool
        run_generation != @stop_generation.get(:acquire)
      end

      def paused?
        @state.paused?
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

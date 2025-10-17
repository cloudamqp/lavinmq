require "../cron"
require "../message"
require "../rough_time"
require "log"

module LavinMQ
  module ScheduledJob
    Log = LavinMQ::Log.for "scheduled-job"

    enum State
      Starting
      Running
      Stopped
      Terminated
      Error
    end

    class Runner
      include SortableJSON

      getter name, state, vhost
      getter exchange, routing_key, body, schedule
      getter? enabled
      getter last_run : Time?
      getter next_run : Time?
      getter error_message : String?
      @fiber : Fiber
      @closed : Channel(Nil)
      @cron : Cron

      def initialize(@name : String, @vhost : VHost,
                     @exchange : String, @routing_key : String,
                     @body : String, @schedule : String,
                     @enabled : Bool = true)
        @state = State::Starting
        @cron = Cron.new(@schedule)
        @last_run = nil
        @next_run = nil
        @error_message = nil
        @closed = Channel(Nil).new
        @fiber = spawn run_loop
      end

      private def run_loop
        @state = State::Running
        calculate_next_run

        loop do
          break if @state.terminated?

          if nr = @next_run
            delay = (nr - Time.utc).total_seconds
            if delay > 0
              select
              when @closed.receive?
                break
              when timeout(delay.seconds)
                publish_message if @enabled
              end
            else
              # We're past the scheduled time, publish immediately
              publish_message if @enabled
            end
            calculate_next_run
          else
            # If no next run time, wait for close
            @closed.receive?
            break
          end
        end
      rescue ex : Exception
        @state = State::Error
        @error_message = ex.message
        Log.error { "Scheduled job #{@name} failed: #{ex.inspect_with_backtrace}" }
      ensure
        @state = State::Terminated
        Log.info { "Scheduled job #{@name} terminated" }
      end

      private def calculate_next_run
        @next_run = @cron.next(Time.utc)
      rescue ex
        @error_message = "Invalid CRON expression: #{ex.message}"
        @state = State::Error
        @next_run = nil
      end

      private def publish_message
        msg = Message.new(
          exchange_name: @exchange,
          routing_key: @routing_key,
          body: @body
        )

        if @vhost.publish(msg)
          @last_run = Time.utc
          Log.info { "Published scheduled message: job=#{@name} exchange=#{@exchange} routing_key=#{@routing_key}" }
        else
          Log.warn { "Failed to publish scheduled message: job=#{@name} exchange=#{@exchange} (exchange not found)" }
        end
      rescue ex
        Log.error { "Error publishing scheduled message: job=#{@name} error=#{ex.message}" }
        @error_message = ex.message
      end

      def update(@exchange : String, @routing_key : String,
                 @body : String, @schedule : String,
                 @enabled : Bool = true)
        old_cron = @cron
        @cron = Cron.new(@schedule)
        calculate_next_run
        Log.info { "Updated scheduled job #{@name}" }
      rescue ex
        @cron = old_cron
        raise ex
      end

      def enable
        @enabled = true
        Log.info { "Enabled scheduled job #{@name}" }
      end

      def disable
        @enabled = false
        Log.info { "Disabled scheduled job #{@name}" }
      end

      def trigger
        Log.info { "Manually triggering scheduled job #{@name}" }
        publish_message
      end

      def close
        return if @state.terminated?
        @state = State::Stopped
        @closed.close
      end

      def details_tuple
        {
          name:          @name,
          vhost:         @vhost.name,
          exchange:      @exchange,
          routing_key:   @routing_key,
          body:          @body,
          schedule:      @schedule,
          enabled:       @enabled,
          state:         @state.to_s.downcase,
          last_run:      @last_run.try(&.to_rfc3339),
          next_run:      @next_run.try(&.to_rfc3339),
          error_message: @error_message,
        }
      end
    end
  end
end

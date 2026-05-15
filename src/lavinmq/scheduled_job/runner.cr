require "digest/sha1"
require "amq-protocol"
require "../sortable_json"
require "../message"
require "../bool_channel"
require "../config"
require "./cron"

module LavinMQ
  module ScheduledJob
    class Runner
      include SortableJSON

      enum State
        Scheduled
        Paused
        Terminated
      end

      Log = LavinMQ::Log.for "scheduled_job"

      getter name : String
      getter cron : Cron
      getter exchange : String
      getter routing_key : String

      @run_count : UInt64 = 0
      @last_run_at : Time? = nil
      @last_error : String? = nil
      @last_duration_ms : Int64? = nil
      @next_run_at : Time? = nil
      @paused_file_path : String
      @paused : BoolChannel
      @stopped : BoolChannel
      @manual_trigger : Channel(Nil)
      @log : ::Log

      def initialize(@name : String, @vhost : VHost, @cron : Cron,
                     @exchange : String, @routing_key : String,
                     @body : String, @properties : AMQ::Protocol::Properties,
                     @on_state_change : Runner -> Nil)
        @log = Log.for(@name)
        @manual_trigger = Channel(Nil).new(1)
        @stopped = BoolChannel.new(false)
        filename = "scheduled_jobs.#{Digest::SHA1.hexdigest @name}.paused"
        @paused_file_path = File.join(Config.instance.data_dir, filename)
        @paused = BoolChannel.new(File.exists?(@paused_file_path))
      end

      def restore_stats(run_count : UInt64, last_run_at : Time?,
                        last_error : String?, last_duration_ms : Int64?) : Nil
        @run_count = run_count
        @last_run_at = last_run_at
        @last_error = last_error
        @last_duration_ms = last_duration_ms
      end

      def state : State
        return State::Terminated if @stopped.value
        return State::Paused if @paused.value
        State::Scheduled
      end

      def run : Nil
        ::Log.context.set(name: @name, vhost: @vhost.name)
        loop do
          return if @stopped.value
          if @paused.value
            @next_run_at = nil
            select
            when @paused.when_false.receive?
            when @stopped.when_true.receive?
              return
            end
            next
          end
          next_at = @cron.next_after(Time.utc)
          @next_run_at = next_at
          delay = next_at - Time.utc
          delay = 1.millisecond if delay < 1.millisecond
          select
          when timeout delay
            fire!
          when @manual_trigger.receive?
            fire!
          when @stopped.when_true.receive?
            return
          when @paused.when_true.receive?
            next
          end
        end
      ensure
        @next_run_at = nil
      end

      def run_now : Bool
        return false unless state.scheduled?
        select
        when @manual_trigger.send(nil)
        else
          # a trigger is already queued; collapse
        end
        true
      end

      def pause : Bool
        return false unless state.scheduled?
        File.write(@paused_file_path, @name)
        @paused.set(true)
        @on_state_change.call(self)
        true
      end

      def resume : Bool
        return false unless state.paused?
        delete_paused_file
        @paused.set(false)
        @on_state_change.call(self)
        true
      end

      def terminate : Nil
        return if @stopped.value
        @stopped.set(true)
      end

      def delete : Nil
        terminate
        delete_paused_file
      end

      def run_count : UInt64
        @run_count
      end

      def last_run_at : Time?
        @last_run_at
      end

      def last_error : String?
        @last_error
      end

      def last_duration_ms : Int64?
        @last_duration_ms
      end

      def next_run_at : Time?
        @next_run_at
      end

      def details_tuple
        {
          name:             @name,
          vhost:            @vhost.name,
          exchange:         @exchange,
          routing_key:      @routing_key,
          cron:             @cron.to_s,
          state:            state.to_s,
          run_count:        @run_count,
          last_run_at:      @last_run_at.try(&.to_rfc3339),
          next_run_at:      @next_run_at.try(&.to_rfc3339),
          last_error:       @last_error,
          last_duration_ms: @last_duration_ms,
        }
      end

      private def fire! : Nil
        start = Time.instant
        msg = LavinMQ::Message.new(@exchange, @routing_key, @body, @properties)
        @vhost.publish(msg)
        @run_count &+= 1
        @last_run_at = Time.utc
        @last_duration_ms = (Time.instant - start).total_milliseconds.to_i64
        @last_error = nil
      rescue ex
        @last_error = ex.message
        @last_run_at = Time.utc
        @log.warn(exception: ex) { "Failed to publish" }
      ensure
        @on_state_change.call(self)
      end

      private def delete_paused_file : Nil
        FileUtils.rm(@paused_file_path) if File.exists?(@paused_file_path)
      end
    end
  end
end

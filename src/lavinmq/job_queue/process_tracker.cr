require "./job_data"
require "./stream_reader"

module LavinMQ
  module JobQueue
    class ProcessTracker
      Log = LavinMQ::Log.for("job_queue.processes")

      def initialize(@vhost : VHost)
        @processes = Hash(String, Heartbeat).new
        @last_offset = 0_i64
        @closed = Channel(Nil).new
        @mutex = Mutex.new
      end

      def start
        spawn do
          Log.info { "Starting process tracker for vhost #{@vhost.name}" }
          loop do
            select
            when @closed.receive?
              Log.info { "Process tracker stopped" }
              break
            when timeout(2.seconds)
              update_processes
            end
          end
        end
      end

      def close
        @closed.close
      end

      def active_processes : Array(Heartbeat)
        @mutex.synchronize do
          @processes.values.to_a
        end
      end

      def total_busy_workers : Int32
        @mutex.synchronize do
          @processes.values.sum(&.busy)
        end
      end

      def total_workers : Int32
        @mutex.synchronize do
          @processes.values.sum(&.concurrency)
        end
      end

      def process_count : Int32
        @mutex.synchronize do
          @processes.size
        end
      end

      def find_process(identity : String) : Heartbeat?
        @mutex.synchronize do
          @processes[identity]?
        end
      end

      private def update_processes
        queue = @vhost.queues["heartbeats"]?
        return unless queue
        return unless queue.is_a?(AMQP::Stream)

        begin
          StreamReader.each_message(queue, @last_offset) do |msg, offset|
            @last_offset = offset

            begin
              heartbeat = Heartbeat.from_json(String.new(msg.body))

              @mutex.synchronize do
                @processes[heartbeat.identity] = heartbeat
              end
            rescue ex : JSON::ParseException
              Log.error { "Failed to parse heartbeat: #{ex.message}" }
            end
          end

          # Remove stale processes (no heartbeat in 60s)
          cleanup_stale_processes
        rescue ex : Exception
          Log.error(exception: ex) { "Error updating processes" }
        end
      end

      private def cleanup_stale_processes
        now = Time.utc
        stale_count = 0

        @mutex.synchronize do
          @processes.reject! do |identity, heartbeat|
            if heartbeat.stale?(now)
              stale_count += 1
              Log.debug { "Removing stale process: #{identity}" }
              true
            else
              false
            end
          end
        end

        Log.debug { "Cleaned up #{stale_count} stale processes" } if stale_count > 0
      end
    end
  end
end

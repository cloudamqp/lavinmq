require "./job_data"
require "./scheduled_job_scheduler"
require "./retry_scheduler"
require "./process_tracker"
require "./metrics_aggregator"
require "./stream_reader"

module LavinMQ
  module JobQueue
    class JobTracker
      Log = LavinMQ::Log.for("job_queue")

      getter scheduled_job_scheduler : ScheduledJobScheduler
      getter retry_scheduler : RetryScheduler
      getter process_tracker : ProcessTracker
      getter metrics_aggregator : MetricsAggregator

      def initialize(@vhost : VHost)
        @scheduled_job_scheduler = ScheduledJobScheduler.new(@vhost)
        @retry_scheduler = RetryScheduler.new(@vhost)
        @process_tracker = ProcessTracker.new(@vhost)
        @metrics_aggregator = MetricsAggregator.new(@vhost)
      end

      def start
        Log.info { "Starting job queue tracker for vhost #{@vhost.name}" }
        @scheduled_job_scheduler.start
        @retry_scheduler.start
        @process_tracker.start
        @metrics_aggregator.start
      end

      def close
        Log.info { "Closing job queue tracker for vhost #{@vhost.name}" }
        @scheduled_job_scheduler.close
        @retry_scheduler.close
        @process_tracker.close
        @metrics_aggregator.close
      end

      # Scheduled jobs operations
      def scheduled_jobs(limit : Int32 = 100, offset : Int64 = 0) : Array(ScheduledJob)
        queue = @vhost.queues["scheduled_jobs"]?
        return [] of ScheduledJob unless queue
        return [] of ScheduledJob unless queue.is_a?(AMQP::Stream)

        jobs = [] of ScheduledJob

        begin
          StreamReader.each_message(queue, offset) do |msg, _|
            break if jobs.size >= limit

            begin
              job = ScheduledJob.from_json(String.new(msg.body))
              jobs << job
            rescue ex : JSON::ParseException
              Log.error { "Failed to parse scheduled job: #{ex.message}" }
            end
          end
        rescue ex : Exception
          Log.error(exception: ex) { "Error reading scheduled jobs" }
        end

        jobs
      end

      def scheduled_jobs_count : Int64
        queue = @vhost.queues["scheduled_jobs"]?
        return 0_i64 unless queue
        return 0_i64 unless queue.is_a?(AMQP::Stream)

        StreamReader.count_messages(queue)
      end

      def find_scheduled_job(jid : String) : ScheduledJob?
        queue = @vhost.queues["scheduled_jobs"]?
        return nil unless queue
        return nil unless queue.is_a?(AMQP::Stream)

        result = StreamReader.find_message(queue) do |msg, _|
          begin
            job = ScheduledJob.from_json(String.new(msg.body))
            job.jid == jid
          rescue
            false
          end
        end

        if result
          msg, _ = result
          ScheduledJob.from_json(String.new(msg.body))
        end
      end

      def trigger_scheduled_job(jid : String) : Bool
        job = find_scheduled_job(jid)
        return false unless job

        # Publish immediately
        job_json = job.job.to_json
        props = AMQ::Protocol::Properties.new(delivery_mode: 2_u8)
        msg = Message.new(
          exchange_name: "jobs.direct",
          routing_key: job.job.queue,
          body: job_json,
          properties: props
        )

        @vhost.publish(msg)
        Log.info { "Manually triggered scheduled job #{jid}" }
        true
      end

      # Retry jobs operations
      def retry_jobs(limit : Int32 = 100, offset : Int64 = 0) : Array(RetryJob)
        queue = @vhost.queues["retries"]?
        return [] of RetryJob unless queue
        return [] of RetryJob unless queue.is_a?(AMQP::Stream)

        jobs = [] of RetryJob

        begin
          StreamReader.each_message(queue, offset) do |msg, _|
            break if jobs.size >= limit

            begin
              job = RetryJob.from_json(String.new(msg.body))
              jobs << job
            rescue ex : JSON::ParseException
              Log.error { "Failed to parse retry job: #{ex.message}" }
            end
          end
        rescue ex : Exception
          Log.error(exception: ex) { "Error reading retry jobs" }
        end

        jobs
      end

      def retry_jobs_count : Int64
        queue = @vhost.queues["retries"]?
        return 0_i64 unless queue
        return 0_i64 unless queue.is_a?(AMQP::Stream)

        StreamReader.count_messages(queue)
      end

      def find_retry_job(jid : String) : RetryJob?
        queue = @vhost.queues["retries"]?
        return nil unless queue
        return nil unless queue.is_a?(AMQP::Stream)

        result = StreamReader.find_message(queue) do |msg, _|
          begin
            job = RetryJob.from_json(String.new(msg.body))
            job.jid == jid
          rescue
            false
          end
        end

        if result
          msg, _ = result
          RetryJob.from_json(String.new(msg.body))
        end
      end

      def retry_job_now(jid : String) : Bool
        job = find_retry_job(jid)
        return false unless job

        # Re-publish immediately
        job.job.retry_count = job.retry_count
        job_json = job.job.to_json
        props = AMQ::Protocol::Properties.new(delivery_mode: 2_u8)

        msg = Message.new(
          exchange_name: "jobs.direct",
          routing_key: job.job.queue,
          body: job_json,
          properties: props
        )

        @vhost.publish(msg)
        Log.info { "Manually retried job #{jid}" }
        true
      end

      def kill_retry_job(jid : String) : Bool
        job = find_retry_job(jid)
        return false unless job

        # Move to dead jobs
        dead_job = DeadJob.new(
          jid: job.jid,
          died_at: Time.utc.to_unix_f,
          retry_count: job.retry_count,
          error_message: job.error_message,
          error_class: job.error_class,
          job: job.job
        )

        publish_to_stream("dead_jobs", dead_job.to_json)
        Log.info { "Moved retry job #{jid} to dead jobs" }
        true
      end

      # Dead jobs operations
      def dead_jobs(limit : Int32 = 100, offset : Int64 = 0) : Array(DeadJob)
        queue = @vhost.queues["dead_jobs"]?
        return [] of DeadJob unless queue
        return [] of DeadJob unless queue.is_a?(AMQP::Stream)

        jobs = [] of DeadJob

        begin
          StreamReader.each_message(queue, offset) do |msg, _|
            break if jobs.size >= limit

            begin
              job = DeadJob.from_json(String.new(msg.body))
              jobs << job
            rescue ex : JSON::ParseException
              Log.error { "Failed to parse dead job: #{ex.message}" }
            end
          end
        rescue ex : Exception
          Log.error(exception: ex) { "Error reading dead jobs" }
        end

        jobs
      end

      def dead_jobs_count : Int64
        queue = @vhost.queues["dead_jobs"]?
        return 0_i64 unless queue
        return 0_i64 unless queue.is_a?(AMQP::Stream)

        StreamReader.count_messages(queue)
      end

      def find_dead_job(jid : String) : DeadJob?
        queue = @vhost.queues["dead_jobs"]?
        return nil unless queue
        return nil unless queue.is_a?(AMQP::Stream)

        result = StreamReader.find_message(queue) do |msg, _|
          begin
            job = DeadJob.from_json(String.new(msg.body))
            job.jid == jid
          rescue
            false
          end
        end

        if result
          msg, _ = result
          DeadJob.from_json(String.new(msg.body))
        end
      end

      def retry_dead_job(jid : String) : Bool
        job = find_dead_job(jid)
        return false unless job

        # Re-publish to original queue with reset retry_count
        job.job.retry_count = 0
        job_json = job.job.to_json
        props = AMQ::Protocol::Properties.new(delivery_mode: 2_u8)

        msg = Message.new(
          exchange_name: "jobs.direct",
          routing_key: job.job.queue,
          body: job_json,
          properties: props
        )

        @vhost.publish(msg)
        Log.info { "Retrying dead job #{jid}" }
        true
      end

      # Helper methods
      private def publish_to_stream(stream_name : String, body : String)
        props = AMQ::Protocol::Properties.new(delivery_mode: 2_u8)
        msg = Message.new(
          exchange_name: "",
          routing_key: stream_name,
          body: body,
          properties: props
        )

        @vhost.publish(msg)
      end
    end
  end
end

require "./job_data"
require "./stream_reader"
require "../message"

module LavinMQ
  module JobQueue
    class RetryScheduler
      Log = LavinMQ::Log.for("job_queue.retry")

      def initialize(@vhost : VHost)
        @last_offset = 0_i64
        @closed = Channel(Nil).new
        @processed_jids = Set(String).new
      end

      def start
        spawn do
          Log.info { "Starting retry scheduler for vhost #{@vhost.name}" }
          loop do
            select
            when @closed.receive?
              Log.info { "Retry scheduler stopped" }
              break
            when timeout(5.seconds)
              process_retry_jobs
            end
          end
        end
      end

      def close
        @closed.close
      end

      private def process_retry_jobs
        queue = @vhost.queues["retries"]?
        return unless queue
        return unless queue.is_a?(AMQP::Stream)

        now = Time.utc.to_unix_f
        retried_count = 0

        begin
          StreamReader.each_message(queue, @last_offset) do |msg, offset|
            @last_offset = offset

            begin
              retry_job = RetryJob.from_json(String.new(msg.body))

              # Skip if already processed
              next if @processed_jids.includes?(retry_job.jid)

              # Check if due for retry
              if retry_job.retry_at <= now
                retry_job.job.retry_count = retry_job.retry_count
                publish_job(retry_job.job)
                @processed_jids.add(retry_job.jid)
                retried_count += 1
                Log.info { "Retrying job #{retry_job.jid} (attempt #{retry_job.retry_count}) to queue #{retry_job.job.queue}" }
              end

              # Only process a batch per iteration
              break if retried_count >= 100
            rescue ex : JSON::ParseException
              Log.error { "Failed to parse retry job: #{ex.message}" }
            end
          end

          # Cleanup processed jids set periodically
          if @processed_jids.size > 10_000
            @processed_jids.clear
          end
        rescue ex : Exception
          Log.error(exception: ex) { "Error processing retry jobs" }
        end
      end

      private def publish_job(job : JobPayload)
        job_json = job.to_json

        props = AMQ::Protocol::Properties.new(delivery_mode: 2_u8) # persistent
        msg = Message.new(
          exchange_name: "jobs.direct",
          routing_key: job.queue,
          body: job_json,
          properties: props
        )

        @vhost.publish(msg)
      end
    end
  end
end

require "./job_data"
require "./stream_reader"
require "../message"

module LavinMQ
  module JobQueue
    class ScheduledJobScheduler
      Log = LavinMQ::Log.for("job_queue.scheduled")

      def initialize(@vhost : VHost)
        @last_offset = 0_i64
        @closed = Channel(Nil).new
        @processed_jids = Set(String).new
      end

      def start
        spawn do
          Log.info { "Starting scheduled job scheduler for vhost #{@vhost.name}" }
          loop do
            select
            when @closed.receive?
              Log.info { "Scheduled job scheduler stopped" }
              break
            when timeout(5.seconds)
              process_scheduled_jobs
            end
          end
        end
      end

      def close
        @closed.close
      end

      private def process_scheduled_jobs
        queue = @vhost.queues["scheduled_jobs"]?
        return unless queue
        return unless queue.is_a?(AMQP::Stream)

        now = Time.utc.to_unix_f
        published_count = 0

        begin
          StreamReader.each_message(queue, @last_offset) do |msg, offset|
            @last_offset = offset

            begin
              scheduled_job = ScheduledJob.from_json(String.new(msg.body))

              # Skip if already processed
              next if @processed_jids.includes?(scheduled_job.jid)

              # Check if due
              if scheduled_job.scheduled_at <= now
                publish_job(scheduled_job.job)
                @processed_jids.add(scheduled_job.jid)
                published_count += 1
                Log.info { "Published scheduled job #{scheduled_job.jid} to queue #{scheduled_job.job.queue}" }
              end

              # Only process a batch per iteration
              break if published_count >= 100
            rescue ex : JSON::ParseException
              Log.error { "Failed to parse scheduled job: #{ex.message}" }
            end
          end

          # Cleanup processed jids set periodically
          if @processed_jids.size > 10_000
            @processed_jids.clear
          end
        rescue ex : Exception
          Log.error(exception: ex) { "Error processing scheduled jobs" }
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

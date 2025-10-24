require "./job_data"
require "./stream_reader"

module LavinMQ
  module JobQueue
    class MetricsAggregator
      Log = LavinMQ::Log.for("job_queue.metrics")

      def initialize(@vhost : VHost)
        @buckets = Hash(String, MetricBucket).new
        @last_offset = 0_i64
        @closed = Channel(Nil).new
        @mutex = Mutex.new
      end

      def start
        spawn do
          Log.info { "Starting metrics aggregator for vhost #{@vhost.name}" }
          loop do
            select
            when @closed.receive?
              Log.info { "Metrics aggregator stopped" }
              break
            when timeout(10.seconds)
              aggregate_metrics
            end
          end
        end
      end

      def close
        @closed.close
      end

      def stats_for_job(job_class : String, period : Time::Span = 24.hours) : MetricBucket?
        cutoff = Time.utc - period
        cutoff_timestamp = cutoff.to_unix_f

        result = MetricBucket.new

        @mutex.synchronize do
          @buckets.each do |key, bucket|
            # Key format: "JobClass:bucket_timestamp"
            parts = key.split(":")
            next unless parts[0] == job_class
            next unless parts[1]?.try(&.to_f64?) || 0.0 > cutoff_timestamp

            result.count += bucket.count
            result.total_ms += bucket.total_ms
            result.success += bucket.success
            result.failed += bucket.failed
          end
        end

        result.count > 0 ? result : nil
      end

      def overall_stats(period : Time::Span = 24.hours) : MetricBucket
        cutoff = Time.utc - period
        cutoff_timestamp = cutoff.to_unix_f

        result = MetricBucket.new

        @mutex.synchronize do
          @buckets.each do |key, bucket|
            parts = key.split(":")
            timestamp = parts[1]?.try(&.to_f64?) || 0.0
            next unless timestamp > cutoff_timestamp

            result.count += bucket.count
            result.total_ms += bucket.total_ms
            result.success += bucket.success
            result.failed += bucket.failed
          end
        end

        result
      end

      def top_jobs(period : Time::Span = 24.hours, limit : Int32 = 10) : Array({String, MetricBucket})
        cutoff = Time.utc - period
        cutoff_timestamp = cutoff.to_unix_f

        job_stats = Hash(String, MetricBucket).new

        @mutex.synchronize do
          @buckets.each do |key, bucket|
            parts = key.split(":")
            job_class = parts[0]
            timestamp = parts[1]?.try(&.to_f64?) || 0.0
            next unless timestamp > cutoff_timestamp

            job_stats[job_class] ||= MetricBucket.new
            job_stats[job_class].count += bucket.count
            job_stats[job_class].total_ms += bucket.total_ms
            job_stats[job_class].success += bucket.success
            job_stats[job_class].failed += bucket.failed
          end
        end

        job_stats.to_a.sort_by { |_, bucket| -bucket.count }.first(limit)
      end

      def processed_count(since : Time = Time.utc.at_beginning_of_day) : Int64
        since_timestamp = since.to_unix_f
        count = 0_i64

        @mutex.synchronize do
          @buckets.each do |key, bucket|
            parts = key.split(":")
            timestamp = parts[1]?.try(&.to_f64?) || 0.0
            count += bucket.count if timestamp >= since_timestamp
          end
        end

        count
      end

      def failed_count(since : Time = Time.utc.at_beginning_of_day) : Int64
        since_timestamp = since.to_unix_f
        count = 0_i64

        @mutex.synchronize do
          @buckets.each do |key, bucket|
            parts = key.split(":")
            timestamp = parts[1]?.try(&.to_f64?) || 0.0
            count += bucket.failed if timestamp >= since_timestamp
          end
        end

        count
      end

      private def aggregate_metrics
        queue = @vhost.queues["job_metrics"]?
        return unless queue
        return unless queue.is_a?(AMQP::Stream)

        metrics_processed = 0

        begin
          StreamReader.each_message(queue, @last_offset) do |msg, offset|
            @last_offset = offset

            begin
              metric = JobMetric.from_json(String.new(msg.body))

              # Create 1-minute bucket key
              bucket_time = (metric.timestamp / 60).to_i * 60
              bucket_key = "#{metric.job_class}:#{bucket_time}"

              @mutex.synchronize do
                bucket = @buckets[bucket_key] ||= MetricBucket.new

                bucket.count += 1
                bucket.total_ms += metric.duration_ms

                case metric.status
                when "success"
                  bucket.success += 1
                when "failed"
                  bucket.failed += 1
                end
              end

              metrics_processed += 1
            rescue ex : JSON::ParseException
              Log.error { "Failed to parse metric: #{ex.message}" }
            end

            # Process up to 1000 metrics per iteration
            break if metrics_processed >= 1000
          end

          Log.debug { "Processed #{metrics_processed} metrics" } if metrics_processed > 0

          # Cleanup old buckets (older than 7 days)
          cleanup_old_buckets
        rescue ex : Exception
          Log.error(exception: ex) { "Error aggregating metrics" }
        end
      end

      private def cleanup_old_buckets
        cutoff = Time.utc - 7.days
        cutoff_timestamp = cutoff.to_unix_f
        removed_count = 0

        @mutex.synchronize do
          @buckets.reject! do |key, _|
            parts = key.split(":")
            timestamp = parts[1]?.try(&.to_f64?) || 0.0

            if timestamp < cutoff_timestamp
              removed_count += 1
              true
            else
              false
            end
          end
        end

        Log.debug { "Cleaned up #{removed_count} old metric buckets" } if removed_count > 0
      end
    end
  end
end

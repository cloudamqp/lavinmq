require "json"

module LavinMQ
  module JobQueue
    # Represents a scheduled job entry in the scheduled_jobs stream
    class ScheduledJob
      include JSON::Serializable

      property type : String
      property jid : String
      property scheduled_at : Float64
      property job : JobPayload

      def initialize(@jid : String, @scheduled_at : Float64, @job : JobPayload)
        @type = "scheduled_job"
      end

      def due?(now : Time = Time.utc) : Bool
        @scheduled_at <= now.to_unix_f
      end
    end

    # Represents a job in retry queue
    class RetryJob
      include JSON::Serializable

      property type : String
      property jid : String
      property retry_at : Float64
      property retry_count : Int32
      property failed_at : Float64
      property error_message : String
      property error_class : String
      property job : JobPayload

      def initialize(@jid : String, @retry_at : Float64, @retry_count : Int32,
                     @failed_at : Float64, @error_message : String,
                     @error_class : String, @job : JobPayload)
        @type = "retry"
      end

      def due?(now : Time = Time.utc) : Bool
        @retry_at <= now.to_unix_f
      end
    end

    # Represents a dead job entry
    class DeadJob
      include JSON::Serializable

      property type : String
      property jid : String
      property died_at : Float64
      property retry_count : Int32
      property error_message : String
      property error_class : String
      property backtrace : String?
      property job : JobPayload

      def initialize(@jid : String, @died_at : Float64, @retry_count : Int32,
                     @error_message : String, @error_class : String,
                     @job : JobPayload, @backtrace : String? = nil)
        @type = "dead_job"
      end
    end

    # The actual job payload
    class JobPayload
      include JSON::Serializable

      property jid : String?
      property class : String
      property args : Array(JSON::Any)
      property queue : String
      property retry : Int32
      property retry_count : Int32?
      property enqueued_at : Float64
      property tags : Array(String)?
      property backtrace : Int32?

      def initialize(@class : String, @args : Array(JSON::Any), @queue : String,
                     @retry : Int32, @enqueued_at : Float64,
                     @jid : String? = nil, @retry_count : Int32? = nil,
                     @tags : Array(String)? = nil, @backtrace : Int32? = nil)
      end
    end

    # Worker process heartbeat
    class Heartbeat
      include JSON::Serializable

      property type : String
      property identity : String
      property hostname : String
      property pid : Int32
      property tag : String
      property concurrency : Int32
      property busy : Int32
      property beat : Float64
      property rss_kb : Int64?
      property queues : Array(String)
      property version : String

      def initialize(@identity : String, @hostname : String, @pid : Int32,
                     @tag : String, @concurrency : Int32, @busy : Int32,
                     @beat : Float64, @queues : Array(String), @version : String,
                     @rss_kb : Int64? = nil)
        @type = "heartbeat"
      end

      def stale?(now : Time = Time.utc, timeout : Time::Span = 60.seconds) : Bool
        beat_time = Time.unix(@beat.to_i64)
        now - beat_time > timeout
      end
    end

    # Job execution metric
    class JobMetric
      include JSON::Serializable

      property type : String
      property timestamp : Float64
      property job_class : String
      property queue : String
      property duration_ms : Int32
      property status : String
      property worker_identity : String

      def initialize(@timestamp : Float64, @job_class : String, @queue : String,
                     @duration_ms : Int32, @status : String, @worker_identity : String)
        @type = "job_metric"
      end
    end

    # Aggregated metrics bucket
    class MetricBucket
      include JSON::Serializable

      property count : Int64 = 0_i64
      property total_ms : Int64 = 0_i64
      property success : Int64 = 0_i64
      property failed : Int64 = 0_i64

      def initialize
        @count = 0_i64
        @total_ms = 0_i64
        @success = 0_i64
        @failed = 0_i64
      end

      def avg_duration_ms : Float64
        return 0.0 if count == 0
        total_ms.to_f / count.to_f
      end

      def success_rate : Float64
        return 0.0 if count == 0
        success.to_f / count.to_f
      end
    end
  end
end

module LavinMQ
  module SQS
    # Process-wide SQS counters for the Prometheus `/metrics` endpoint.
    #
    # They live outside the vhosts so they stay monotonic when a vhost is
    # deleted (per-vhost numbers are in `Broker` and exported by the
    # `sqs_metrics` detailed family). The action label comes from a client
    # supplied header, so only known action names become labels; anything
    # else is counted as `unknown` to keep the number of series bounded.
    module Metrics
      ACTIONS = {
        "AddPermission", "CancelMessageMoveTask", "ChangeMessageVisibility",
        "ChangeMessageVisibilityBatch", "CreateQueue", "DeleteMessage",
        "DeleteMessageBatch", "DeleteQueue", "GetQueueAttributes", "GetQueueUrl",
        "ListDeadLetterSourceQueues", "ListMessageMoveTasks", "ListQueueTags",
        "ListQueues", "PurgeQueue", "ReceiveMessage", "RemovePermission",
        "SendMessage", "SendMessageBatch", "SetQueueAttributes",
        "StartMessageMoveTask", "TagQueue", "UntagQueue",
      }
      UNKNOWN_ACTION = "unknown"
      OK             = "OK"

      record RequestKey, action : String, code : String

      @@lock = Mutex.new
      @@requests = Hash(RequestKey, UInt64).new
      @@durations = Hash(String, Float64).new
      @@enabled = Atomic(Bool).new(false)
      @@messages_sent = Atomic(UInt64).new(0_u64)
      @@messages_received = Atomic(UInt64).new(0_u64)
      @@messages_deleted = Atomic(UInt64).new(0_u64)
      @@visibility_timeouts = Atomic(UInt64).new(0_u64)
      @@long_polls_waiting = Atomic(Int64).new(0_i64)

      # Set when an SQS server is created; the exporter only writes SQS
      # metrics then, so a server without SQS exports no SQS series.
      def self.enabled? : Bool
        @@enabled.get(:relaxed)
      end

      def self.enable : Nil
        @@enabled.set(true, :relaxed)
      end

      # Maps an X-Amz-Target header to a bounded action label
      def self.action_label(target : String?) : String
        return UNKNOWN_ACTION if target.nil?
        action = target.lchop("AmazonSQS.")
        ACTIONS.includes?(action) ? action : UNKNOWN_ACTION
      end

      def self.record_request(action : String, code : String, duration : Time::Span) : Nil
        key = RequestKey.new(action, code)
        @@lock.synchronize do
          @@requests[key] = @@requests.fetch(key, 0_u64) &+ 1
          @@durations[action] = @@durations.fetch(action, 0.0) + duration.total_seconds
        end
      end

      # Snapshots, so the exporter writes to the client without holding the lock
      def self.requests : Array({RequestKey, UInt64})
        @@lock.synchronize { @@requests.to_a }
      end

      def self.durations : Array({String, Float64})
        @@lock.synchronize { @@durations.to_a }
      end

      {% for name in %w[messages_sent messages_received messages_deleted visibility_timeouts] %}
        def self.{{ name.id }} : UInt64
          @@{{ name.id }}.get(:relaxed)
        end

        def self.add_{{ name.id }}(count : Int = 1) : Nil
          @@{{ name.id }}.add(count.to_u64, :relaxed)
        end
      {% end %}

      def self.long_polls_waiting : Int64
        @@long_polls_waiting.get(:relaxed)
      end

      # Counts a receiver blocked in a long poll for the duration of the block
      def self.long_polling(&)
        @@long_polls_waiting.add(1, :relaxed)
        begin
          yield
        ensure
          @@long_polls_waiting.sub(1, :relaxed)
        end
      end
    end
  end
end

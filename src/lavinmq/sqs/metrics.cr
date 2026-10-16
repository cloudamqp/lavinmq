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
      OTHER_CODE     = "other"

      # Result codes counted per action: OK, every SQS error code (a spec
      # checks that each `SQS::Error` is listed) and transport failures
      CODES = {
        OK, "ConnectionError",
        "AccessDeniedException", "BatchEntryIdsNotDistinct", "BatchRequestTooLong",
        "EmptyBatchRequest", "InternalError", "InvalidAction", "InvalidAddress",
        "InvalidAttributeName", "InvalidAttributeValue", "InvalidBatchEntryId",
        "InvalidClientTokenId", "InvalidMessageContents", "InvalidParameterValue",
        "MissingAuthenticationToken", "MissingParameter", "OverLimit",
        "PurgeQueueInProgress", "QueueDoesNotExist", "QueueNameExists",
        "ReceiptHandleIsInvalid", "SerializationException",
        "TooManyEntriesInBatchRequest", "UnsupportedOperation",
        OTHER_CODE,
      }

      ACTION_LABELS = ACTIONS.to_a.push(UNKNOWN_ACTION)
      # Keyed by the full X-Amz-Target header value so a lookup allocates nothing
      ACTION_INDEX         = ACTIONS.each_with_index.to_h { |action, i| {"AmazonSQS.#{action}", i} }
      UNKNOWN_ACTION_INDEX = ACTIONS.size
      CODE_INDEX           = CODES.each_with_index.to_h { |code, i| {code, i} }
      OTHER_CODE_INDEX     = CODES.size - 1

      # A heap-allocated counter, so an array of them can be updated in place
      class Counter
        @value = Atomic(UInt64).new(0_u64)

        def add(n : UInt64) : Nil
          @value.add(n, :relaxed)
        end

        def get : UInt64
          @value.get(:relaxed)
        end
      end

      # One counter per (action, code) pair and one duration counter (in
      # nanoseconds) per action, allocated once: recording a request is two
      # lock-free atomic adds.
      @@requests = Array(Counter).new(ACTION_LABELS.size * CODES.size) { Counter.new }
      @@durations = Array(Counter).new(ACTION_LABELS.size) { Counter.new }
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

      # Maps an X-Amz-Target header to a bounded action index
      def self.action_index(target : String?) : Int32
        return UNKNOWN_ACTION_INDEX if target.nil?
        ACTION_INDEX.fetch(target, UNKNOWN_ACTION_INDEX)
      end

      def self.code_index(code : String) : Int32
        CODE_INDEX.fetch(code, OTHER_CODE_INDEX)
      end

      def self.record_request(action : Int32, code : Int32, duration : Time::Span) : Nil
        @@requests[action * CODES.size + code].add(1_u64)
        @@durations[action].add(duration.total_nanoseconds.to_u64)
      end

      # Yields every (action, code) pair that has been counted
      def self.each_request(& : String, String, UInt64 ->) : Nil
        ACTION_LABELS.each_with_index do |action, a|
          CODES.each_with_index do |code, c|
            count = @@requests[a * CODES.size + c].get
            yield action, code, count if count > 0
          end
        end
      end

      # Yields the total seconds spent per action, for actions seen
      def self.each_duration(& : String, Float64 ->) : Nil
        ACTION_LABELS.each_with_index do |action, a|
          nanos = @@durations[a].get
          yield action, nanos / 1_000_000_000 if nanos > 0
        end
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

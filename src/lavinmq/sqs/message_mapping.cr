require "uuid"
require "amq-protocol"
require "./message_attributes"
require "../message"

module LavinMQ
  module SQS
    # Maps SQS message fields onto AMQP properties and headers, and back.
    module MessageMapping
      ATTRIBUTES_HEADER     = "x-sqs-attributes"
      SENT_TIMESTAMP_HEADER = "x-sqs-sent-timestamp"
      SENDER_ID_HEADER      = "x-sqs-sender-id"
      GROUP_ID_HEADER       = "x-sqs-message-group-id"
      DEDUP_ID_HEADER       = "x-sqs-deduplication-id"
      TRACE_HEADER          = "x-sqs-trace-header"
      DELAY_HEADER          = "x-delay"
      CONTENT_TYPE          = "text/plain; charset=utf-8"

      def self.build_properties(message_id : String, sent_ms : Int64, sender_id : String,
                                attrs : Array(MessageAttribute), system_attrs : Array(MessageAttribute),
                                group_id : String?, dedup_id : String?, delay_ms : Int64) : AMQ::Protocol::Properties
        headers = AMQ::Protocol::Table.new
        headers[SENT_TIMESTAMP_HEADER] = sent_ms
        headers[SENDER_ID_HEADER] = sender_id
        headers[ATTRIBUTES_HEADER] = MessageAttribute.to_table(attrs) unless attrs.empty?
        if trace = system_attrs.find { |a| a.name == "AWSTraceHeader" }
          headers[TRACE_HEADER] = trace.string_value
        end
        headers[GROUP_ID_HEADER] = group_id if group_id
        headers[DEDUP_ID_HEADER] = dedup_id if dedup_id
        headers[DELAY_HEADER] = delay_ms if delay_ms > 0
        AMQ::Protocol::Properties.new(
          content_type: CONTENT_TYPE,
          headers: headers,
          delivery_mode: 2_u8,
          message_id: message_id,
          timestamp: sent_ms // 1000,
        )
      end

      # A message as read back from a queue, before it is handed to a client
      struct Received
        getter message_id : String
        getter body : String
        getter sent_timestamp : Int64
        getter sender_id : String?
        getter group_id : String?
        getter dedup_id : String?
        getter trace_header : String?
        getter attributes : Array(MessageAttribute)

        def initialize(@message_id, @body, @sent_timestamp, @sender_id, @group_id, @dedup_id,
                       @trace_header, @attributes)
        end

        # Copies everything out of the envelope; the message bytes live in the
        # store and must not be referenced after the get block returns.
        def self.from(env : Envelope, queue_name : String) : Received
          msg = env.message
          props = msg.properties
          headers = props.headers
          message_id = props.message_id || stable_message_id(queue_name, env.segment_position)
          sent = headers.try(&.[SENT_TIMESTAMP_HEADER]?).as?(Int64) ||
                 props.timestamp_raw.try(&.*(1000)) ||
                 msg.timestamp
          new(
            message_id,
            String.new(msg.body),
            sent,
            headers.try(&.[SENDER_ID_HEADER]?).as?(String),
            headers.try(&.[GROUP_ID_HEADER]?).as?(String),
            headers.try(&.[DEDUP_ID_HEADER]?).as?(String),
            headers.try(&.[TRACE_HEADER]?).as?(String),
            MessageAttribute.from_table(headers.try(&.[ATTRIBUTES_HEADER]?)),
          )
        end

        # Messages published over AMQP without a message id still need an id
        # that is identical every time the same message is received.
        private def self.stable_message_id(queue_name : String, sp : SegmentPosition) : String
          UUID.v5_url("lavinmq:sqs:#{queue_name}/#{sp.segment}/#{sp.position}").to_s
        end
      end
    end
  end
end

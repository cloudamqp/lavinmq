require "amq-protocol"
require "../rough_time"

module LavinMQ
  module QueueFilter
    # Stamps origin metadata onto a Properties record so that a message
    # diverted out of its source queue (by `move_to`, `duplicate_to`, or
    # any later operational trigger) carries enough information for a
    # downstream queue — including the planned `x-queue-type: replay`
    # destination — to identify and replay it.
    #
    # Returns a new Properties; the caller is responsible for building a
    # Message around it. Headers are cloned so the original Properties
    # (and its IO::Memory-backed Table) is not mutated.
    module SourceStamp
      HEADER_QUEUE       = "x-source-queue"
      HEADER_EXCHANGE    = "x-source-exchange"
      HEADER_ROUTING_KEY = "x-source-routing-key"
      HEADER_TIMESTAMP   = "x-source-timestamp"
      HEADER_RULE_ID     = "x-source-rule-id"

      # Reused by feature-2 (poison-pill / nack) when it lands; kept here
      # so both features stamp the same header.
      HEADER_DELIVERY_COUNT = "x-delivery-count"

      def self.stamp(properties : AMQ::Protocol::Properties, source_queue : String,
                     source_exchange : String, source_routing_key : String,
                     rule_id : String? = nil,
                     delivery_count : Int32 = 0) : AMQ::Protocol::Properties
        headers = properties.headers.try(&.clone) || AMQ::Protocol::Table.new
        headers[HEADER_QUEUE] = source_queue
        headers[HEADER_EXCHANGE] = source_exchange
        headers[HEADER_ROUTING_KEY] = source_routing_key
        headers[HEADER_TIMESTAMP] = RoughTime.unix_ms
        headers[HEADER_RULE_ID] = rule_id if rule_id
        headers[HEADER_DELIVERY_COUNT] = delivery_count if delivery_count > 0
        AMQ::Protocol::Properties.new(
          content_type: properties.content_type,
          content_encoding: properties.content_encoding,
          headers: headers,
          delivery_mode: properties.delivery_mode,
          priority: properties.priority,
          correlation_id: properties.correlation_id,
          reply_to: properties.reply_to,
          expiration: properties.expiration,
          message_id: properties.message_id,
          timestamp: properties.timestamp_raw,
          type: properties.type,
          user_id: properties.user_id,
          app_id: properties.app_id,
          reserved1: properties.reserved1,
        )
      end
    end
  end
end

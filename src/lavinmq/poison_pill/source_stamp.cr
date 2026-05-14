require "amq-protocol"
require "../rough_time"

module LavinMQ
  module PoisonPill
    # Stamps origin metadata onto a Properties record when a message is
    # diverted out of its source queue by the redelivery threshold or
    # the nack-to-quarantine intake. The header schema deliberately
    # mirrors the cross-feature `x-source-*` set so a diverted message
    # can land cleanly in an `x-queue-type: replay` queue or in any
    # other inspection / replay tooling that consumes the same names.
    module SourceStamp
      HEADER_SOURCE_QUEUE       = "x-source-queue"
      HEADER_SOURCE_EXCHANGE    = "x-source-exchange"
      HEADER_SOURCE_ROUTING_KEY = "x-source-routing-key"
      HEADER_SOURCE_TIMESTAMP   = "x-source-timestamp"
      HEADER_DELIVERY_COUNT     = "x-delivery-count"

      # Returns a new Properties; the caller is responsible for
      # building a Message around it. Headers are cloned so the
      # original Properties (and its IO::Memory-backed Table) is not
      # mutated.
      def self.stamp(properties : AMQ::Protocol::Properties, source_queue : String,
                     source_exchange : String, source_routing_key : String,
                     delivery_count : Int32 = 0) : AMQ::Protocol::Properties
        headers = properties.headers.try(&.clone) || AMQ::Protocol::Table.new
        headers[HEADER_SOURCE_QUEUE] = source_queue
        headers[HEADER_SOURCE_EXCHANGE] = source_exchange
        headers[HEADER_SOURCE_ROUTING_KEY] = source_routing_key
        headers[HEADER_SOURCE_TIMESTAMP] = RoughTime.unix_ms
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

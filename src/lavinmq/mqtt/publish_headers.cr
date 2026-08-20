require "../amqp"
require "./protocol"

module LavinMQ
  module MQTT
    # Round-trips v5 PUBLISH properties through AMQP message headers, so a v5
    # publisher's properties survive store-and-forward to a v5 subscriber.
    #
    # Header-only mapping (no AMQP-native slots like content_type/reply_to - that
    # cross-protocol mapping is a separate concern). `topic_alias` and
    # `subscription_identifiers` are intentionally not carried (rejected on
    # ingress / not supported). Retained messages don't carry properties yet -
    # the retain store keeps only the body (follow-up).
    module PublishHeaders
      PAYLOAD_FORMAT_INDICATOR = "mqtt.payload_format_indicator"
      MESSAGE_EXPIRY_INTERVAL  = "mqtt.message_expiry_interval"
      RESPONSE_TOPIC           = "mqtt.response_topic"
      CORRELATION_DATA         = "mqtt.correlation_data"
      CONTENT_TYPE             = "mqtt.content_type"
      USER_PROPERTIES          = "mqtt.user_properties"

      # Stash the present (non-nil) v5 properties into `headers`. A v3 or
      # property-less publish adds nothing.
      def self.store(props : Protocol::PublishProperties, headers : AMQP::Table) : Nil
        if v = props.payload_format_indicator
          headers[PAYLOAD_FORMAT_INDICATOR] = v
        end
        if v = props.message_expiry_interval
          headers[MESSAGE_EXPIRY_INTERVAL] = v
        end
        if v = props.response_topic
          headers[RESPONSE_TOPIC] = v
        end
        if v = props.correlation_data
          headers[CORRELATION_DATA] = v
        end
        if v = props.content_type
          headers[CONTENT_TYPE] = v
        end
        return if props.user_properties.empty?
        # An array of {key,value} tables preserves order and duplicate keys
        # [MQTT-3.3.2-18], which a flat Table/Hash would lose.
        headers[USER_PROPERTIES] = props.user_properties.map do |(key, value)|
          pair = AMQP::Table.new
          pair["key"] = key
          pair["value"] = value
          pair
        end
      end

      # Rebuild v5 properties from `headers`; returns empty properties when none
      # were stashed.
      #
      # `headers` is not necessarily something `store` wrote: an AMQP client can
      # bind `mqtt.<client-id>` to `amq.topic` and publish arbitrary values. Every
      # field must therefore degrade to nil rather than raise - a raise here lands
      # in `Session#get_packet`, which requeues and re-raises, so the message
      # poisons the queue on every redelivery.
      def self.restore(headers : AMQP::Table?) : Protocol::PublishProperties
        props = Protocol::PublishProperties.new
        return props unless headers
        props.payload_format_indicator = headers[PAYLOAD_FORMAT_INDICATOR]?.as?(Bool)
        props.message_expiry_interval = fetch_u32?(headers, MESSAGE_EXPIRY_INTERVAL)
        props.response_topic = fetch_topic?(headers, RESPONSE_TOPIC)
        props.correlation_data = headers[CORRELATION_DATA]?.as?(Bytes)
        props.content_type = headers[CONTENT_TYPE]?.as?(String)
        entries = headers[USER_PROPERTIES]?
        props.user_properties = restore_user_properties(entries) if entries.is_a?(Array)
        props
      end

      # Every four-byte-int property goes through here, so a value outside
      # UInt32 drops the property instead of raising `OverflowError`. `Int` has
      # no `to_u32?`, hence the explicit bounds check.
      private def self.fetch_u32?(headers : AMQP::Table, key : String) : UInt32?
        i = headers[key]?.as?(Int) || return
        return unless i >= 0 && i <= UInt32::MAX
        i.to_u32
      end

      # A Response Topic must be a topic name, not a filter [MQTT-3.3.2-14], and
      # nothing validated it on the way in via an AMQP header.
      private def self.fetch_topic?(headers : AMQP::Table, key : String) : String?
        topic = headers[key]?.as?(String) || return
        return if topic.includes?('#') || topic.includes?('+')
        topic
      end

      private def self.restore_user_properties(entries : Array) : Array({String, String})
        result = Array({String, String}).new(entries.size)
        entries.each do |entry|
          pair = entry.as?(AMQP::Table) || next
          key = pair["key"]?.as?(String)
          value = pair["value"]?.as?(String)
          result << {key, value} if key && value
        end
        result
      end
    end
  end
end

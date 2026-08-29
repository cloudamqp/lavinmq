require "../amqp"

module LavinMQ
  module MQTT
    EXCHANGE      = "mqtt.default"
    QOS_HEADER    = "mqtt.qos"
    RETAIN_HEADER = "mqtt.retain"

    QOS0_ARGUMENTS = AMQP::Table.new({QOS_HEADER => 0u8})
    QOS1_ARGUMENTS = AMQP::Table.new({QOS_HEADER => 1u8})

    # The binding arguments that carry the given QoS, as one of the two shared,
    # treat-as-read-only constants above, so no table is allocated per subscription.
    # QoS 2 isn't supported and is granted as QoS 1, hence anything above 0 maps to
    # QOS1_ARGUMENTS.
    def self.qos_arguments(qos : UInt8) : AMQP::Table
      qos.zero? ? QOS0_ARGUMENTS : QOS1_ARGUMENTS
    end

    # The QoS carried in binding arguments, the inverse of `qos_arguments`.
    #
    # Any integer type is accepted: bindings made over the HTTP API or imported
    # from a definitions file don't go through the MQTT protocol parser, so the
    # header isn't necessarily a `UInt8`. The value is granted as the closest
    # supported QoS, never higher than what we can deliver: QoS 2 as QoS 1
    # [MQTT-3.9.3-1], a missing, negative or non-integer value as QoS 0.
    def self.qos(arguments : AMQP::Table?) : UInt8
      qos = arguments.try { |args| args[QOS_HEADER]?.as?(Int) } || 0
      qos.clamp(0, 1).to_u8
    end
  end
end

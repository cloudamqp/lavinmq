require "../amqp"

module LavinMQ
  module MQTT
    EXCHANGE      = "mqtt.default"
    QOS_HEADER    = "mqtt.qos"
    RETAIN_HEADER = "mqtt.retain"

    QOS0_ARGUMENTS = AMQP::Table.new({QOS_HEADER => 0u8})
    QOS1_ARGUMENTS = AMQP::Table.new({QOS_HEADER => 1u8})
    QOS2_ARGUMENTS = AMQP::Table.new({QOS_HEADER => 2u8})

    # The binding arguments that carry the given QoS, as one of the three shared,
    # treat-as-read-only constants above, so no table is allocated per
    # subscription. Callers clamp to 0..2 first, so the `else` arm is only ever
    # reached with 2.
    def self.qos_arguments(qos : UInt8) : AMQP::Table
      case qos
      when 0u8 then QOS0_ARGUMENTS
      when 1u8 then QOS1_ARGUMENTS
      else          QOS2_ARGUMENTS
      end
    end

    # The QoS carried in binding arguments, the inverse of `qos_arguments`.
    #
    # Any integer type is accepted: bindings made over the HTTP API or imported
    # from a definitions file don't go through the MQTT protocol parser, so the
    # header isn't necessarily a `UInt8`. Anything above 2 is granted as QoS 2,
    # the ceiling MQTT 3.1.1 defines; a missing or non-integer value as QoS 0.
    def self.qos(arguments : AMQP::Table?) : UInt8
      qos = arguments.try { |args| args[QOS_HEADER]?.as?(Int) } || 0
      qos.clamp(0, 2).to_u8
    end
  end
end

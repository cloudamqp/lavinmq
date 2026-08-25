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
  end
end

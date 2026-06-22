module LavinMQ
  module MQTT
    EXCHANGE      = "mqtt.default"
    QOS_HEADER    = "mqtt.qos"
    RETAIN_HEADER = "mqtt.retain"

    QOS0_ARGUMENTS = AMQP::Table.new({QOS_HEADER => 0u8})
    QOS1_ARGUMENTS = AMQP::Table.new({QOS_HEADER => 1u8})
  end
end

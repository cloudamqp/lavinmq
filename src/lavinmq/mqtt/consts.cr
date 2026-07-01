module LavinMQ
  module MQTT
    EXCHANGE      = "mqtt.default"
    QOS_HEADER    = "mqtt.qos"
    RETAIN_HEADER = "mqtt.retain"
    # Highest QoS LavinMQ supports. QoS 2 is not implemented, so this is the
    # value advertised in the v5 CONNACK, enforced on inbound v5 PUBLISH, and
    # used to clamp delivery QoS.
    MAX_QOS = 1u8
  end
end

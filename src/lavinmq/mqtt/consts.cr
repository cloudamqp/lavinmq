module LavinMQ
  module MQTT
    EXCHANGE      = "mqtt.default"
    QOS_HEADER    = "mqtt.qos"
    RETAIN_HEADER = "mqtt.retain"
    # Highest QoS LavinMQ supports. QoS 2 is not implemented, so this is the
    # value advertised in the v5 CONNACK, enforced on inbound v5 PUBLISH, and
    # used to clamp delivery QoS.
    MAX_QOS = 1u8
    # Queue argument carrying a session's Session Expiry Interval, in seconds.
    # It lives in the arguments because that is the only part of the
    # Queue::Declare frame definitions_store persists that can hold a UInt32.
    SESSION_EXPIRY_ARG = "x-mqtt-session-expiry"
  end
end

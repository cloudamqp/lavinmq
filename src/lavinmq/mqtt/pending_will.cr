require "./protocol"
require "./broker"

module LavinMQ
  module MQTT
    # A will waiting out its Will Delay Interval on the session [MQTT-3.1.2-8].
    # Carries the broker so the session publishes through `Broker#publish`,
    # which applies the retain store, without holding a `Broker` of its own.
    record PendingWill, packet : Protocol::Publish, broker : Broker, deadline : Time::Instant
  end
end

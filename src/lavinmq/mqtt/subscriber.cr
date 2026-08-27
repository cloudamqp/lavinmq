require "../message"

module LavinMQ
  module MQTT
    # An entry in a vhost's `SubscriptionTree`. Implemented by `MQTT::Session`,
    # which holds one MQTT client's messages, and by
    # `AMQP::MqttTopicExchange`, which forwards to the AMQP queues bound to it.
    #
    # Reference types only: the tree stores entries in hashes that
    # `compare_by_identity`.
    module Subscriber
      # Deliver a message that matched `filter`, one of the filters this
      # subscriber is registered under. Returns true if the message was
      # accepted, in which case the caller rewinds `msg.body_io` before handing
      # it to the next subscriber.
      abstract def deliver(msg : Message, filter : String) : Bool
    end
  end
end

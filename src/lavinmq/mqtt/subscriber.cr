module LavinMQ
  module MQTT
    # Entry type of a vhost's SubscriptionTree. The tree's entry hashes compare
    # by identity, so including types must be reference types.
    module Subscriber
      # Called once per matching filter when an MQTT message is published.
      # Returns true if the message was accepted by at least one receiver.
      abstract def deliver(msg : Message, filter : String) : Bool
    end
  end
end

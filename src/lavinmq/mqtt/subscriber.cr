module LavinMQ
  module MQTT
    # Entry type of a vhost's SubscriptionTree. The tree's entry hashes compare
    # by identity, so including types must be reference types.
    module Subscriber
      # Called once per matching filter when an MQTT message is published.
      # `publish_seq` is the same for every call made for one publish, so a
      # subscriber matched by several filters can tell the calls apart from a
      # new publish. Returns true if the message was accepted by at least one
      # receiver.
      abstract def deliver(msg : Message, filter : String, publish_seq : UInt64) : Bool
    end
  end
end

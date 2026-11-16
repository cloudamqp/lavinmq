require "./publish_context"

module LavinMQ
  module MQTT
    # Entry type of a vhost's SubscriptionTree. The tree's entry hashes compare
    # by identity, so including types must be reference types.
    module Subscriber
      # Called once per matching filter when an MQTT message is published.
      # `ctx` is the publishing client's scratch state, reset per publish, so a
      # subscriber matched by several filters can tell the calls apart from a
      # new publish. Returns true if the message was accepted by at least one
      # receiver.
      abstract def deliver(msg : Message, filter : String, ctx : PublishContext) : Bool
    end
  end
end

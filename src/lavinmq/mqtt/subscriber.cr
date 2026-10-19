require "./publish_context"

module LavinMQ
  module MQTT
    # Entry type of a vhost's SubscriptionTree, which compares entries with
    # `==` and `hash`. Both are identity here, whatever an including class
    # inherits, so two subscribers are never taken for one subscription.
    # Including types must be reference types.
    module Subscriber
      def ==(other : Subscriber) : Bool
        same?(other)
      end

      def hash(hasher)
        hasher.reference(self)
      end

      # Called once per matching filter when an MQTT message is published.
      # `ctx` is the publishing client's scratch state, reset per publish, so a
      # subscriber matched by several filters can tell the calls apart from a
      # new publish. Returns true if the message was accepted by at least one
      # receiver.
      abstract def deliver(msg : Message, filter : String, ctx : PublishContext) : Bool
    end
  end
end

module LavinMQ
  module MQTT
    # What the delivery path needs to know about a subscription: the granted QoS
    # plus the two v5 subscription options that outlive the SUBSCRIBE.
    #
    # Retain Handling is deliberately not a field. It only decides whether the
    # retain store is replayed during the SUBSCRIBE itself, is never consulted at
    # delivery, and so is never carried in the tree or the binding arguments.
    #
    # A `record` rather than a bare struct on purpose: `Hash::Entry#clone` calls
    # `value.clone`, and the structural `==` is what `Session#subscribe` compares
    # to decide whether a re-subscribe actually changed anything.
    record SubscriptionOptions,
      qos : UInt8 = 0u8,
      no_local : Bool = false,
      retain_as_published : Bool = false do
      def no_local?
        @no_local
      end

      def retain_as_published?
        @retain_as_published
      end
    end
  end
end

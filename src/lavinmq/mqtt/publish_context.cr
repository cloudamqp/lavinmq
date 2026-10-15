require "../amqp/destination"

module LavinMQ
  module MQTT
    class PublishContext
      record Outcome, handled : Bool, counted_unroutable : Bool

      getter queues = Set(AMQP::Queue).new
      getter exchanges = Set(AMQP::Exchange).new
      getter outcomes = Hash(AMQP::Exchange, Outcome).new

      def reset : self
        @outcomes.clear
        self
      end
    end
  end
end

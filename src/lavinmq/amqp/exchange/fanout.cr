require "./exchange"

module LavinMQ
  module AMQP
    class FanoutExchange < Exchange
      @bindings = Array(Destination).new

      def type : String
        "fanout"
      end

      def bindings_details : Iterator(BindingDetails)
        @bindings.each.map do |d|
          binding_key = BindingKey.new("")
          BindingDetails.new(name, vhost.name, binding_key, d)
        end
      end

      def bind(destination : Destination, routing_key, headers = nil)
        return false if @bindings.includes? destination
        @bindings.push destination
        binding_key = BindingKey.new("")
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key, headers = nil)
        return false unless @bindings.delete destination
        binding_key = BindingKey.new("")
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)
        delete if @auto_delete && @bindings.empty?
        true
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
        @bindings.each do |destination|
          yield destination
        end
      end
    end
  end
end

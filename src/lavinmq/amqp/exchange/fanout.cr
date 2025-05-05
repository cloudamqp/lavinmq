require "./exchange"

module LavinMQ
  module AMQP
    class FanoutExchange < Exchange
      @bindings = Array({Destination, BindingKey}).new

      def type : String
        "fanout"
      end

      def bindings_details : Iterator(BindingDetails)
        @bindings.each.map do |d, binding_key|
          BindingDetails.new(name, vhost.name, binding_key, d)
        end
      end

      def bind(destination : Destination, routing_key, arguments = nil)
        binding_key = BindingKey.new(routing_key, arguments)
        binding = {destination, binding_key}
        return false if @bindings.includes? binding
        @bindings.push binding
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key, arguments = nil)
        return false unless @bindings.delete({destination, arguments})
        binding_key = BindingKey.new("", arguments)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)
        delete if @auto_delete && @bindings.empty?
        true
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
        @bindings.each do |destination, _binding_key|
          yield destination
        end
      end
    end
  end
end

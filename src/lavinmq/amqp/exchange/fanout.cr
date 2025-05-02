require "./exchange"

module LavinMQ
  module AMQP
    class FanoutExchange < Exchange
      @bindings = Array({Destination, AMQP::Table?}).new

      def type : String
        "fanout"
      end

      def bindings_details : Iterator(BindingDetails)
        @bindings.each.map do |d, arguments|
          binding_key = BindingKey.new("", arguments)
          BindingDetails.new(name, vhost.name, binding_key, d)
        end
      end

      def bind(destination : Destination, routing_key, arguments = nil)
        binding = {destination, arguments}
        return false if @bindings.includes? binding
        @bindings.push binding
        binding_key = BindingKey.new("", arguments)
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
        @bindings.each do |destination, _arguments|
          yield destination
        end
      end
    end
  end
end

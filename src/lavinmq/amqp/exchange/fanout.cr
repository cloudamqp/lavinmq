require "./exchange"

module LavinMQ
  module AMQP
    class FanoutExchange < Exchange
      # Replaced, never mutated, so publishers on other threads can route
      # with the set they read while a bind or unbind publishes a new one
      @bindings = Atomic(BindingSet).new(BindingSet.empty)

      def type : String
        "fanout"
      end

      def bindings_details : Array(BindingDetails)
        bindings = @bindings.get(:acquire)
        Array(BindingDetails).new(bindings.size).tap do |details|
          bindings.each do |e|
            details << BindingDetails.new(name, vhost.name, e.binding_key, e.destination)
          end
        end
      end

      def binding_count : Int32
        @bindings.get(:acquire).size
      end

      def bind(destination : Destination, routing_key, arguments = nil)
        validate_delayed_binding!(destination)
        binding_key = BindingKey.new(routing_key, arguments)
        bindings = @bindings.get(:acquire).add(destination, binding_key) || return false
        @bindings.set(bindings, :release)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key, arguments = nil)
        binding_key = BindingKey.new(routing_key, arguments)
        current = @bindings.get(:acquire)
        bindings = current.delete(destination, binding_key)
        return false if bindings.same?(current)
        @bindings.set(bindings, :release)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)
        delete if @auto_delete && bindings.empty?
        true
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : (LavinMQ::Queue | LavinMQ::Exchange) ->)
        @bindings.get(:acquire).each_destination { |d| yield d }
      end
    end
  end
end

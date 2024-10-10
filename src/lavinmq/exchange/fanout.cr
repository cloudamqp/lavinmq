require "./exchange"

module LavinMQ
  class FanoutExchange < Exchange
    @bindings = Set(Destination).new

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
      return false unless @bindings.add? destination
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

    protected def bindings(routing_key, headers) : Iterator(Destination)
      @bindings.each
    end
  end
end

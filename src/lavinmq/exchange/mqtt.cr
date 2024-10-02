require "./exchange"

module LavinMQ
  class MQTTExchange < Exchange
    # record Binding, topic_filter : String, qos : UInt8

    @bindings = Hash(BindingKey, Set(Destination)).new do |h, k|
      h[k] = Set(Destination).new
    end

    def type : String
      "mqtt"
    end

    def bindings_details : Iterator(BindingDetails)
      @bindings.each.flat_map do |binding_key, ds|
        ds.each.map do |d|
          BindingDetails.new(name, vhost.name, binding_key, d)
        end
      end
    end


      # TODO: For each matching binding, get the QoS and write to message header "qos"
      # Should be done in the MQTTExchange not in this super class
      # if a = arguments[msg.routing_key]
      # msg.properties.header["qos"] = q.qos
      # end
      # use delivery_mode on properties instead, always set to 1 or 0


    def bind(destination : Destination, topic_filter : String, arguments = nil) : Bool
      # binding = Binding.new(topic_filter, arguments["x-mqtt-qos"])
      binding_key = BindingKey.new(topic_filter, arguments)
      return false unless @bindings[binding_key].add? destination
      data = BindingDetails.new(name, vhost.name, binding_key, destination)
      notify_observers(ExchangeEvent::Bind, data)
      true
    end

    def unbind(destination : Destination, routing_key, headers = nil) : Bool
      binding_key = BindingKey.new(routing_key, arguments)
      rk_bindings = @bindings[binding_key]
      return false unless rk_bindings.delete destination
      @bindings.delete binding_key if rk_bindings.empty?

      data = BindingDetails.new(name, vhost.name, binding_key, destination)
      notify_observers(ExchangeEvent::Unbind, data)

      delete if @auto_delete && @bindings.each_value.all?(&.empty?)
      true
    end

    protected def bindings : Iterator(Destination)
      @bindings.values.each.flat_map(&.each)
    end

    protected def bindings(routing_key, headers) : Iterator(Destination)
      binding_key = BindingKey.new(routing_key, headers)
      matches(binding_key).each
    end

    private def matches(binding_key : BindingKey) : Iterator(Destination)
      @bindings.each.select do |binding, destinations|
        msg_qos = binding_key.arguments.try { |a| a["qos"]?.try(&.as(UInt8)) } || 0
        binding_qos = binding.arguments.try { |a| a["x-mqtt-pos"]?.try(&.as(UInt8)) } || 0

        # Use Jons tree finder..
        binding.routing_key == binding_key.routing_key && msg_qos >= binding_qos
      end.flat_map { |_, v| v.each }
    end
  end
end

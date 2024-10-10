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


    # TODO: we can probably clean this up a bit
    def publish(msg : Message, immediate : Bool,
            queues : Set(Queue) = Set(Queue).new,
            exchanges : Set(Exchange) = Set(Exchange).new) : Int32
      @publish_in_count += 1
      headers = msg.properties.headers
      find_queues(msg.routing_key, headers, queues, exchanges)
      if queues.empty?
        @unroutable_count += 1
        return 0
      end
      return 0 if immediate && !queues.any? &.immediate_delivery?

      count = 0
      queues.each do |queue|
        qos = 0_u8
        bindings_details.each do |binding_detail|
          next unless binding_detail.destination == queue
          next unless arg = binding_detail.binding_key.arguments
          next unless qos_value = arg["x-mqtt-qos"]?
          qos = qos_value.try &.as(UInt8)
        end
        msg.properties.delivery_mode = qos

        if queue.publish(msg)
          @publish_out_count += 1
          count += 1
          msg.body_io.seek(-msg.bodysize.to_i64, IO::Seek::Current) # rewind
        end
      end
      count
    end

    def bind(destination : Destination, routing_key : String, headers = nil) : Bool
      # binding = Binding.new(topic_filter, arguments["x-mqtt-qos"])

      # TODO: build spec for this early return
      raise LavinMQ::Exchange::AccessRefused.new(self) unless destination.is_a?(MQTT::Session)
      binding_key = BindingKey.new(routing_key, headers)
      return false unless @bindings[binding_key].add? destination
      data = BindingDetails.new(name, vhost.name, binding_key, destination)
      notify_observers(ExchangeEvent::Bind, data)
      true
    end

    def unbind(destination : Destination, routing_key, headers = nil) : Bool
      binding_key = BindingKey.new(routing_key, headers)
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

        # Use Jons tree finder..
        binding.routing_key == binding_key.routing_key
      end.flat_map { |_, v| v.each }
    end
  end
end

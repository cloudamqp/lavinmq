module AvalancheMQ
  module BindingHelpers
    private def bindings(vhost)
      vhost.exchanges.values.flat_map do |e|
        e.bindings_details.map { |b| map_binding(b) }
      end
    end

    private def map_binding(b)
      key_tuple = {b[:routing_key].as(String), b[:arguments].as?(Hash(String, AMQP::Field))}
      b.merge({properties_key: hash_key(key_tuple)})
    end

    private def binding_for_props(context, source, destination, props)
      binding = source.bindings.select do |k, v|
        v.includes?(destination) && hash_key(k) == props
      end.first?
      unless binding
        type = destination.is_a?(Queue) ? "queue" : "exchange"
        not_found(context, "Binding '#{props}' on exchange '#{source.name}' -> #{type} '#{destination.name}' does not exist")
      end
      binding
    end

    private def hash_key(key : Tuple(String, Hash(String, AMQP::Field)?))
      if key[1].nil? || key[1].try &.empty?
        key[0]
      else
        hsh = Base64.urlsafe_encode(key[1].to_s)
        "#{key[0]}~#{hsh}"
      end
    end

    private def unbind_prop(source : Queue | Exchange, destination : Queue | Exchange, key : String)
      key = source.bindings.keys.find do |k|
        hash_key(k) == key
      end
      source.unbind(destination, key[0], key[1]) if key
    end
  end
end

require "./amqp"

module LavinMQ
  struct BindingKey
    getter routing_key : String
    getter arguments : AMQP::Table? = nil

    def initialize(@routing_key : String, @arguments : AMQP::Table? = nil)
    end

    def properties_key
      if (args = arguments) && !args.empty?
        @hsh ||= begin
          hsh = args.to_h
          Base64.urlsafe_encode(hsh.keys.sort!.map { |k| "#{k}:#{hsh[k]}" }.join(","))
        end
        return "#{routing_key}~#{@hsh}"
      end
      return "~" if routing_key.empty?
      "#{routing_key}"
    end

    def_hash properties_key
  end
end

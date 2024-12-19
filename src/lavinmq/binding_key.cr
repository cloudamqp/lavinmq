require "./amqp"

module LavinMQ
  struct BindingKey
    getter routing_key : String
    getter arguments : AMQP::Table? = nil

    def initialize(@routing_key : String, @arguments : AMQP::Table? = nil)
    end

    def properties_key
      if arguments.nil? || arguments.try(&.empty?)
        routing_key.empty? ? "~" : routing_key
      else
        hsh = Base64.urlsafe_encode(arguments.to_s)
        "#{routing_key}~#{hsh}"
      end
    end
  end
end

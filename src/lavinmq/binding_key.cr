require "./amqp"
require "./binding_arguments"

module LavinMQ
  struct BindingKey
    getter routing_key : String
    getter arguments : BindingArguments?

    def self.new(routing_key : String, arguments : AMQP::Table? = nil)
      new routing_key, arguments.try &.to_h
    end

    def initialize(@routing_key : String, @arguments : BindingArguments? = nil)
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

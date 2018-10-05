module AvalancheMQ
  module ResourceHelpers
    private def parse_arguments(body)
      if args = body["arguments"]?.try(&.as_h?)
        AMQP.cast_to_field(args).as Hash(String, AMQP::Field)
      else
        Hash(String, AMQP::Field).new
      end
    end
  end
end

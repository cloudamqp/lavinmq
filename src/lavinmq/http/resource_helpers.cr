module LavinMQ
  module HTTP
    module ResourceHelpers
      private def parse_arguments(body) : Hash(String, AMQP::Field)
        if args = body["arguments"]?.try(&.as_h?)
          AMQP::Properties.cast_to_field(args).as Hash(String, AMQP::Field)
        else
          Hash(String, AMQP::Field).new
        end
      end
    end
  end
end

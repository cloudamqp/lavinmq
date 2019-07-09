module AvalancheMQ
  module HTTP
    module ResourceHelpers
      private def parse_arguments(body) : AMQP::Table
        if args = body["arguments"]?.try(&.as_h?)
          AMQP::Properties.cast_to_field(args).as AMQP::Table
        else
          AMQP::Table.new
        end
      end
    end
  end
end

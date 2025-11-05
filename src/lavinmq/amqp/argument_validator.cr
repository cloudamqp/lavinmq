module LavinMQ
  module AMQP
    module ArgumentValidator
      abstract def validate!(header : String, value : AMQP::Field) : Nil

      def raise_invalid!(msg)
        raise LavinMQ::Error::PreconditionFailed.new(msg)
      end
    end
  end
end

require "./argument_validator/*"

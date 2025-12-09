module LavinMQ
  module AMQP
    # Includer should override one of the validate! methods
    module ArgumentValidator
      def validate!(header : String, value : AMQP::Field) : Nil
      end

      def validate!(header : String, value : AMQP::Field, arguments : AMQP::Table) : Nil
        validate!(header, value)
      end

      def raise_invalid!(msg)
        raise LavinMQ::Error::PreconditionFailed.new(msg)
      end
    end
  end
end

require "./argument_validator/*"

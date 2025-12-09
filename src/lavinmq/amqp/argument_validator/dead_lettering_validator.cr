module LavinMQ
  module AMQP
    module ArgumentValidator
      struct DeadLetteringValidator
        include ArgumentValidator

        @dlx : String?

        def initialize(arguments : AMQP::Table)
          @dlx = arguments["x-dead-letter-exchange"]?.try &.as?(String)
        end

        def validate!(header : String, value : AMQP::Field) : Nil
          return if value.nil?
          dlrk = value.as?(String)
          raise_invalid!("#{header} header not a string") if dlrk.nil?
          if @dlx.nil?
            raise_invalid!("x-dead-letter-exchange required if x-dead-letter-routing-key is defined")
          end
          nil
        end
      end
    end
  end
end

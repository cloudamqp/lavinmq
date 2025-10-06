module LavinMQ
  module AMQP
    module ArgumentValidator
      # Most validators only requires header and value...
      def validate!(header : String, value : AMQP::Field) : Nil
        raise NotImplementedError.new("validate! must be implemented in #{self.class}")
      end

      # ...but if they need arguments this method can be overloaded
      def validate!(header : String, value : AMQP::Field, arguments) : Nil
        validate!(header, value)
      end

      def raise_invalid!(msg)
        raise LavinMQ::Error::PreconditionFailed.new(msg)
      end

      struct DeadLetteringValidator
        include ArgumentValidator

        @dlx : String?

        def initialize(arguments : AMQP::Table)
          @dlx = arguments["x-dead-letter-exchange"]?.try &.as?(String)
        end

        def validate!(header : String, value : AMQP::Field, arguments) : Nil
          return if value.nil?
          dlrk = value.as?(String)
          raise_invalid!("#{header} header not a string") if dlrk.nil?
          if @dlx.nil?
            raise_invalid!("x-dead-letter-exchange required if x-dead-letter-routing-key is defined")
          end
          nil
        end
      end

      # Validates that value is an Int and optionally greater and/or smaller than given valuea
      struct IntValidator
        include ArgumentValidator

        def initialize(@min_value : Int32? = nil, @max_value : Int32? = nil)
        end

        def validate!(header : String, value : AMQP::Field) : Nil
          return if value.nil?
          int_value = value.as?(Int) || raise_invalid!("#{header} header not an integer")
          if (min_value = @min_value) && int_value < min_value
            raise_invalid!("#{header} header less than minimum value #{min_value}")
          end
          if (max_value = @max_value) && int_value > max_value
            raise_invalid!("#{header} header greater than maximum value #{max_value}")
          end
          nil
        end
      end

      struct StringValidator
        include ArgumentValidator

        def validate!(header : String, value : AMQP::Field) : Nil
          return if value.nil?
          value.as?(String) || raise_invalid!("#{header} header not a string")
          nil
        end
      end

      struct BoolValidator
        include ArgumentValidator

        def validate!(header : String, value : AMQP::Field) : Nil
          return if value.nil?
          value.as?(Bool) || raise_invalid!("#{header} header not a boolean")
          nil
        end
      end
    end
  end
end

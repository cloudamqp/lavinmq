require "./filter"

module LavinMQ::AMQP
  struct KVFilter
    include StreamFilter

    getter key : String
    getter value : String

    def initialize(@key : String, @value : String)
    end

    def match?(headers : AMQP::Table) : Bool
      if msg_value = headers[@key]?
        msg_value.to_s == @value
      else
        false
      end
    end
  end
end

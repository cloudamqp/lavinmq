require "./filter"

module LavinMQ::AMQP
  struct KVFilter
    include StreamFilter

    getter key : String
    getter value : String

    def initialize(@key : String, @value : String)
    end

    def match?(headers : AMQP::Table) : Bool
      msg_value = headers[@key]?
      msg_value.to_s == @value
    end
  end
end

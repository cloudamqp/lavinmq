require "./filter"

module LavinMQ::AMQP
  struct XStreamFilter
    include StreamFilter

    getter value : String

    def initialize(@value : String)
    end

    def match?(headers : AMQP::Table) : Bool
      if msg_filter_values = headers.try &.fetch("x-stream-filter-value", nil).try &.to_s
        msg_filter_values.split(',') do |msg_filter_value|
          return true if msg_filter_value == @value
        end
      end
      false
    end
  end
end

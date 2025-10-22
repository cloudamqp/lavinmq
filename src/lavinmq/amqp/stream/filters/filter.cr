module LavinMQ::AMQP
  module StreamFilter
    abstract def match?(headers : AMQP::Table) : Bool
  end
end

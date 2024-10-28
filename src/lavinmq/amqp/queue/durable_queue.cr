require "./queue"

module LavinMQ::AMQP
  class DurableQueue < Queue
    def durable?
      true
    end
  end
end

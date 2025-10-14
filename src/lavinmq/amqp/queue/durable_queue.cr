require "./queue"

module LavinMQ::AMQP
  class DurableQueue < Queue
    def self.create(vhost : VHost, name : String,
                    exclusive : Bool = false, auto_delete : Bool = false,
                    arguments : AMQP::Table = AMQP::Table.new)
      self.validate_arguments!(arguments)
      new vhost, name, exclusive, auto_delete, arguments
    end

    def durable?
      true
    end
  end
end

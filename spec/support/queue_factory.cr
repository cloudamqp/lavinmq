module LavinMQ
  class QueueFactory
    def self.make(
      vhost : VHost,
      name : String,
      exclusive : Bool = false,
      auto_delete : Bool = false,
      durable : Bool = true,
      arguments : AMQP::Table = AMQP::Table.new,
    )
      frame = AMQP::Frame::Queue::Declare.new(
        1u16,
        0u16,
        name,
        passive: false,
        durable: durable,
        exclusive: exclusive,
        auto_delete: auto_delete,
        no_wait: true,
        arguments: arguments
      )
      self.make(vhost, frame)
    end
  end
end

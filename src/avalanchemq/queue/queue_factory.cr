require "./queue"
require "./priority_queue"
require "./durable_queue"

module AvalancheMQ
  class QueueFactory
    def self.make(vhost : VHost, frame : AMQP::Frame)
      if frame.durable
        make_durable(vhost, frame)
      else
        make_queue(vhost, frame)
      end
    end

    private def self.make_durable(vhost, frame)
      if frame.arguments["x-max-priority"]?
        DurablePriorityQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments.to_h)
      else
        DurableQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments.to_h)
      end
    end

    private def self.make_queue(vhost, frame)
      if frame.arguments["x-max-priority"]?
        PriorityQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments.to_h)
      else
        Queue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments.to_h)
      end
    end
  end
end

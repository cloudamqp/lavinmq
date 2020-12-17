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
      if prio_queue? frame
        DurablePriorityQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments.to_h)
      else
        DurableQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments.to_h)
      end
    end

    private def self.make_queue(vhost, frame)
      if prio_queue? frame
        PriorityQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments.to_h)
      else
        Queue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments.to_h)
      end
    end

    private def self.prio_queue?(frame)
      value = frame.arguments["x-max-priority"]?
      return false unless value
      p_value = value.as?(Int)
      raise Error::PreconditionFailed.new("x-max-priority must be an int32") unless p_value
      ok = 0 <= p_value <= 255
      raise Error::PreconditionFailed.new("x-max-priority must be between 0 and 255") unless ok
      ok
    end
  end
end

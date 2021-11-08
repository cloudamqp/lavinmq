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
      DurableQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments.to_h)
    end

    private def self.make_queue(vhost, frame)
      Queue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments.to_h)
    end

    private def self.prio_queue?(frame)
      if value = frame.arguments["x-max-priority"]?
        p_value = value.as?(Int) || raise Error::PreconditionFailed.new("x-max-priority must be an int")
        unless p_value >= 0 && p_value <= 255
          raise Error::PreconditionFailed.new("x-max-priority must be between 0 and 255")
        end
        true
      end
    end
  end
end

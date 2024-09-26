require "./queue"
require "./priority_queue"
require "./durable_queue"
require "./stream_queue"

module LavinMQ
  class QueueFactory
    def self.make(vhost : VHost, frame : AMQP::Frame)
      if prio_queue?(frame) && stream_queue?(frame)
        raise Error::PreconditionFailed.new("A queue cannot be both a priority queue and a stream queue")
      elsif frame.durable
        make_durable(vhost, frame)
      else
        make_queue(vhost, frame)
      end
    end

    private def self.make_durable(vhost, frame)
      if prio_queue? frame
        DurablePriorityQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments)
      elsif stream_queue? frame
        if frame.exclusive
          raise Error::PreconditionFailed.new("A stream queue cannot be exclusive")
        elsif frame.auto_delete
          raise Error::PreconditionFailed.new("A stream queue cannot be auto-delete")
        end
        StreamQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments)
      else
        check_unsupported_queue_type frame
        DurableQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments)
      end
    end

    private def self.make_queue(vhost, frame)
      if prio_queue? frame
        PriorityQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments)
      elsif stream_queue? frame
        raise Error::PreconditionFailed.new("A stream queue cannot be non-durable")
      else
        check_unsupported_queue_type frame
        Queue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments)
      end
    end

    private def self.prio_queue?(frame) : Bool
      if value = frame.arguments["x-max-priority"]?
        p_value = value.as?(Int) || raise Error::PreconditionFailed.new("x-max-priority must be an int")
        0 <= p_value <= 255 || raise Error::PreconditionFailed.new("x-max-priority must be between 0 and 255")
      else
        false
      end
    end

    private def self.stream_queue?(frame) : Bool
      frame.arguments["x-queue-type"]? == "stream"
    end

    private def self.check_unsupported_queue_type(frame)
      if frame.arguments["x-queue-type"]?
        Log.warn { "The queue type #{frame.arguments["x-queue-type"]} is not supported by LavinMQ and will revert to default" }
      end
    end
  end
end

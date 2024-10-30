require "./amqp/queue"
require "./amqp/queue/priority_queue"
require "./amqp/queue/durable_queue"
require "./amqp/queue/stream_queue"

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
      validate_mqtt_queue_name frame
      if prio_queue? frame
        AMQP::DurablePriorityQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments)
      elsif stream_queue? frame
        if frame.exclusive
          raise Error::PreconditionFailed.new("A stream queue cannot be exclusive")
        elsif frame.auto_delete
          raise Error::PreconditionFailed.new("A stream queue cannot be auto-delete")
        end
        StreamQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments)
      elsif mqtt_session? frame
        MQTT::Session.new(vhost, frame.queue_name, frame.auto_delete, frame.arguments)
      else
        warn_if_unsupported_queue_type frame
        AMQP::DurableQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments)
      end
    end

    private def self.make_queue(vhost, frame)
      validate_mqtt_queue_name frame
      if prio_queue? frame
        AMQP::PriorityQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments)
      elsif stream_queue? frame
        raise Error::PreconditionFailed.new("A stream queue cannot be non-durable")
      elsif mqtt_session? frame
        MQTT::Session.new(vhost, frame.queue_name, frame.auto_delete, frame.arguments)
      else
        warn_if_unsupported_queue_type frame
        AMQP::Queue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments)
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

    private def self.warn_if_unsupported_queue_type(frame)
      if frame.arguments["x-queue-type"]?
        Log.info { "The queue type #{frame.arguments["x-queue-type"]} is not supported by LavinMQ and will be changed to the default queue type" }
      end
    end

    private def self.mqtt_session?(frame) : Bool
      frame.arguments["x-queue-type"]? == "mqtt"
    end

    private def self.validate_mqtt_queue_name(frame)
      if frame.queue_name.starts_with?("mqtt.") && !mqtt_session?(frame)
        raise Error::PreconditionFailed.new("Only MQTT sessions can create queues with the prefix 'mqtt.'")
      end
    end
  end
end

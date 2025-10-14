require "./amqp/queue"
require "./amqp/queue/priority_queue"
require "./amqp/queue/durable_queue"
require "./amqp/stream/stream"
require "./mqtt/session"

module LavinMQ
  class QueueFactory
    def self.make(vhost : VHost, frame : AMQP::Frame)
      if mqtt_session?(frame)
        MQTT::Session.new(vhost, frame.queue_name, frame.auto_delete, frame.arguments)
      else
        if frame.durable
          make_durable(vhost, frame)
        else
          make_queue(vhost, frame)
        end
      end
    end

    private def self.make_durable(vhost, frame)
      if stream_queue? frame
        AMQP::Stream.create(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments)
      else
        warn_if_unsupported_queue_type frame
        if prio_queue? frame
          AMQP::DurablePriorityQueue.create(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments)
        else
          AMQP::DurableQueue.create(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments)
        end
      end
    end

    private def self.make_queue(vhost, frame)
      if stream_queue? frame
        raise Error::PreconditionFailed.new("A stream cannot be non-durable")
      end
      warn_if_unsupported_queue_type frame
      if prio_queue? frame
        AMQP::PriorityQueue.create(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments)
      else
        AMQP::Queue.create(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments)
      end
    end

    private def self.prio_queue?(frame) : Bool
      frame.arguments["x-max-priority"]? != nil
    end

    private def self.stream_queue?(frame) : Bool
      frame.arguments["x-queue-type"]? == "stream"
    end

    private def self.mqtt_session?(frame) : Bool
      frame.arguments["x-queue-type"]? == "mqtt"
    end

    private def self.warn_if_unsupported_queue_type(frame)
      if frame.arguments["x-queue-type"]?
        Log.info { "The queue type #{frame.arguments["x-queue-type"]} is not supported by LavinMQ and will be changed to the default queue type" }
      end
    end
  end
end

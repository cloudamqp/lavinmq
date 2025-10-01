require "./amqp/queue"
require "./amqp/queue/priority_queue"
require "./amqp/queue/durable_queue"
require "./amqp/stream/stream"
require "./mqtt/session"

module LavinMQ
  class QueueFactory
    def self.make(vhost : VHost, frame : AMQP::Frame)
      if prio_queue?(frame) && stream_queue?(frame)
        raise Error::PreconditionFailed.new("A queue cannot be both a priority queue and a stream")
      end
      if mqtt_session?(frame)
        MQTT::Session.new(vhost, frame.queue_name, frame.auto_delete, frame.arguments)
      else
        validate_amqp_arguments!(frame.arguments)
        if frame.durable
          make_durable(vhost, frame)
        else
          make_queue(vhost, frame)
        end
      end
    end

    private def self.make_durable(vhost, frame)
      if prio_queue? frame
        AMQP::DurablePriorityQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments)
      elsif stream_queue? frame
        if frame.exclusive
          raise Error::PreconditionFailed.new("A stream cannot be exclusive")
        elsif frame.auto_delete
          raise Error::PreconditionFailed.new("A stream cannot be auto-delete")
        end
        AMQP::Stream.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments)
      else
        warn_if_unsupported_queue_type frame
        AMQP::DurableQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments)
      end
    end

    private def self.make_queue(vhost, frame)
      if prio_queue? frame
        AMQP::PriorityQueue.new(vhost, frame.queue_name, frame.exclusive, frame.auto_delete, frame.arguments)
      elsif stream_queue? frame
        raise Error::PreconditionFailed.new("A stream cannot be non-durable")
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

    private def self.validate_amqp_arguments!(arguments : AMQP::Table) : Nil
      dlrk = arguments["x-dead-letter-routing-key"]?.try &.as?(String)
      dlx = arguments["x-dead-letter-exchange"]?.try &.as?(String)
      if dlrk && dlx.nil?
        raise LavinMQ::Error::PreconditionFailed.new("x-dead-letter-exchange required if x-dead-letter-routing-key is defined")
      end
      validate_number(arguments, "x-expires", 1)
      validate_number(arguments, "x-max-length")
      validate_number(arguments, "x-max-length-bytes")
      validate_number(arguments, "x-message-ttl")
      validate_number(arguments, "x-delivery-limit")
      validate_number(arguments, "x-consumer-timeout")
    end

    private def self.validate_number(headers, header, min_value = 0)
      value = headers[header]?.try &.as?(Int)
      if min_value == 0
        validate_positive(header, value)
      else
        validate_gt_zero(header, value)
      end
    end

    private def self.validate_positive(header, value) : Nil
      return if value.nil?
      return if value >= 0
      raise LavinMQ::Error::PreconditionFailed.new("#{header} has to be positive")
    end

    private def self.validate_gt_zero(header, value) : Nil
      return if value.nil?
      return if value > 0
      raise LavinMQ::Error::PreconditionFailed.new("#{header} has to be larger than 0")
    end
  end
end

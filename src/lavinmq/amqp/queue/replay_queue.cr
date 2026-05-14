require "./durable_queue"
require "../../replay/stamp"

module LavinMQ::AMQP
  # A queue type whose purpose is operator-facing message review. Every
  # message that lands on a replay queue gets the x-source-* origin
  # headers and an x-replay-id stamped (or filled in from
  # x-first-death-* when a DLX route brings it in). Messages that
  # cannot be stamped — i.e. they have no origin metadata at all — are
  # refused so the queue never contains messages that the management
  # API would be unable to replay.
  #
  # Always durable. Declaring with `durable=false` is rejected by
  # QueueFactory.
  class ReplayQueue < DurableQueue
    def self.create(vhost : VHost, name : String,
                    exclusive : Bool = false, auto_delete : Bool = false,
                    arguments : AMQP::Table = AMQP::Table.new)
      validate_arguments!(arguments)
      new vhost, name, exclusive, auto_delete, arguments
    end

    protected def publish_internal(msg : Message, dlx_tasks : Argument::DeadLettering::Tasks? = nil) : Bool
      stamped = ::LavinMQ::Replay.stamp_intake(msg.properties)
      stamped_msg = Message.new(msg.timestamp, msg.exchange_name, msg.routing_key,
        stamped, msg.bodysize, msg.body_io)
      super(stamped_msg, dlx_tasks)
    rescue ex : LavinMQ::Error::PreconditionFailed
      @log.warn { "Refusing message to replay queue '#{@name}': #{ex.message}" }
      false
    end
  end
end

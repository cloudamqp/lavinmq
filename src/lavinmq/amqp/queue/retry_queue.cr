module LavinMQ::AMQP
  class RetryQueue < Queue
    getter? internal = true

    def self.create(vhost : VHost, primary_name : String, level : Int32, ttl_ms : Int64, durable : Bool)
      name = "#{primary_name}.x-retry.#{level}"
      arguments = AMQP::Table.new({
        "x-message-ttl"             => ttl_ms,
        "x-dead-letter-exchange"    => "",
        "x-dead-letter-routing-key" => primary_name,
      })
      if durable
        DurableRetryQueue.new(vhost, name, false, false, arguments)
      else
        RetryQueue.new(vhost, name, false, false, arguments)
      end
    end

    # Internal queues don't self-expire
    private def queue_expire_loop
    end

    # No policy application for internal retry queues
    private def apply_policy_argument(key : String, value : JSON::Any) : Bool
      false
    end

    # Retry queues reuse the parent Queue's details_tuple
  end

  class DurableRetryQueue < RetryQueue
    def durable?
      true
    end
  end
end

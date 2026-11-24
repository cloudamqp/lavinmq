require "./queue"

module LavinMQ::AMQP
  # The pending queue of a manual exchange (`x-manual-exchange: true`). The
  # exchange parks each message that it gets here, in order, until a client
  # that consumes from this queue decides where the message goes. See
  # Exchange#route_decision and Client::Channel#manual_decision.
  #
  # Unlike other internal queues, clients with read permission can consume
  # from it (basic.consume, basic.get) and ack, reject or nack its messages.
  # They can't publish to it through the default exchange, bind, purge or
  # delete it.
  class ManualExchangeQueue < Queue
    MAX_NAME_LENGTH = 255

    # Headers that only a decision may carry: removed from parked messages
    DECISION_HEADERS = {"x-manual-delivery-tag", "x-manual-route", "x-manual-match", "x-manual-routed-by"}

    getter? internal = true

    def self.queue_name(exchange_name : String) : String
      "amq.manual-#{exchange_name}"
    end

    def self.create(vhost : VHost, exchange_name : String, durable : Bool, max_length : Int64?)
      q_name = queue_name(exchange_name)
      if q_name.bytesize > MAX_NAME_LENGTH
        raise LavinMQ::Error::PreconditionFailed.new("Exchange name too long for a manual exchange")
      end
      arguments = AMQP::Table.new
      if max_length
        arguments["x-max-length"] = max_length
        arguments["x-overflow"] = "reject-publish"
      end
      if durable
        DurableManualExchangeQueue.new(vhost, q_name, false, false, arguments)
      else
        new(vhost, q_name, false, false, arguments)
      end
    end

    def consumable_internal? : Bool
      true
    end

    # Only the manual exchange routes here. The default exchange routes by
    # queue name, which would let a publisher park a message in a pending
    # queue without the exchange.
    def publish(msg : Message) : PublishResult
      return PublishResult::Dropped if msg.exchange_name.empty?
      strip_decision_headers(msg)
      super
    end

    # A parked message must not look like it was decided already
    private def strip_decision_headers(msg : Message) : Nil
      headers = msg.properties.headers || return
      DECISION_HEADERS.each do |key|
        headers.delete(key) if headers.has_key?(key)
      end
    end

    # Policies never apply to internal queues
    private def apply_policy_argument(key : String, value : JSON::Any) : Bool
      false
    end

    # Internal queues never auto-expire (x-expires does not apply)
    private def queue_expire_loop
    end
  end

  class DurableManualExchangeQueue < ManualExchangeQueue
    def durable?
      true
    end
  end
end

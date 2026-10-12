require "./delayed_queue"

module LavinMQ::AMQP
  # This class is only used by delayed exchanges. It can't niehter should be
  # consumed from or published to by clients.
  class DelayedExchangeQueue < DelayedQueue
    @exchange_name : String

    def self.create(vhost : VHost, exchange_name : String, durable : Bool, auto_delete : Bool)
      q_name = "amq.delayed-#{exchange_name}"
      if q_name.bytesize > MAX_NAME_LENGTH
        raise LavinMQ::Error::PreconditionFailed.new("Exchange name too long for a delayed exchange")
      end

      legacy_q_name = "amq.delayed.#{exchange_name}"
      if use_legacy_name?(vhost.data_dir, legacy_q_name)
        q_name = legacy_q_name
      end

      arguments = AMQP::Table.new({
        "x-dead-letter-exchange" => exchange_name,
        "auto-delete"            => auto_delete,
      })
      if durable
        DurableDelayedExchangeQueue.new(vhost, q_name, false, false, arguments)
      else
        DelayedExchangeQueue.new(vhost, q_name, false, false, arguments)
      end
    end

    private def self.use_legacy_name?(vhost_data_dir, legacy_q_name)
      q_dir_name = Digest::SHA1.hexdigest(legacy_q_name)
      Dir.exists?(Path[vhost_data_dir] / q_dir_name)
    end

    protected def initialize(*args)
      super(*args)
      @exchange_name = arguments["x-dead-letter-exchange"]?.try(&.to_s) || raise "Missing x-dead-letter-exchange"
    end

    def delay(msg : Message) : Bool
      return false if @closed
      @msg_store_lock.synchronize do
        @msg_store.push(msg)
      end
      @publish_count.add(1, :relaxed)
      @message_ttl_change.try_send? nil
      ensure_expire_fiber # restart the release fiber if it died on a transient store error
      true
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
    end

    # Overload to not ruin DLX header
    private def expire_msg(env : Envelope, reason : Symbol)
      sp = env.segment_position
      msg = env.message
      @log.debug { "Expiring #{sp} now due to #{reason}" }
      if headers = msg.properties.headers
        headers.delete("x-delay")
        msg.properties.headers = headers
      end
      @vhost.exchange(@exchange_name).route_msg Message.new(msg.timestamp, @exchange_name, msg.routing_key,
        msg.properties, msg.bodysize, IO::Memory.new(msg.body))
      delete_message sp
    end
  end

  class DurableDelayedExchangeQueue < DelayedExchangeQueue
    def durable?
      true
    end
  end
end

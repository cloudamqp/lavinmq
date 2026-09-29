module LavinMQ
  # Something publishes are confirmed to once they are persisted: an AMQP
  # 0-9-1 channel in confirm mode, or an AMQP 1.0 receiving link. The
  # Persister calls back with the highest confirm id that is persisted.
  module PublishConfirmTarget
    # Called from the Persister's thread; must not block or write sockets.
    abstract def enqueue_confirm_ack(msgid : UInt64) : Nil
  end
end

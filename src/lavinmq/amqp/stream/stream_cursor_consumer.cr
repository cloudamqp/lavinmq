require "./stream_cursor"

module LavinMQ::AMQP
  # A consumer reading a Stream through a StreamCursor: AMQP::StreamConsumer,
  # and Endpoint::LocalStreamConsumer for in-process shovels and federation
  # links. The stream wakes it on publish and closes its cursor when it's
  # removed.
  module StreamCursorConsumer
    abstract def cursor : StreamCursor
    abstract def waiting_for_messages?
    abstract def notify_new_message
  end
end

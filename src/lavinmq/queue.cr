require "./amqp/queue/queue"
require "./mqtt/session"

module LavinMQ
  alias Queue = AMQP::Queue | MQTT::Session
end

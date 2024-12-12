require "./exchange/exchange"
require "./queue"

module LavinMQ
  module AMQP
    alias Destination = AMQP::Queue | AMQP::Exchange
  end
end

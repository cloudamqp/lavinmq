require "./exchange/exchange"
require "./queue/queue"

module LavinMQ
  module AMQP
    alias Destination = Queue | Exchange
  end
end

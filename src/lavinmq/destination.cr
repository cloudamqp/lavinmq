require "./exchange"
require "./queue"

module LavinMQ
  alias Destination = LavinMQ::Queue | LavinMQ::Exchange
end

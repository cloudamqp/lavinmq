module LavinMQ
  module Shovel
    abstract class Destination
      abstract def start

      abstract def stop

      abstract def push(msg, source)

      abstract def started? : Bool
    end
  end
end

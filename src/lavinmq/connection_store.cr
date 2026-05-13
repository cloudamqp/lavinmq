require "wait_group"
require "./client/client"
require "./logger"

module LavinMQ
  class ConnectionStore
    def initialize
      @connections = Array(Client).new(512)
    end

    def add(client : Client)
      @connections << client
    end

    def delete(client : Client)
      @connections.delete client
    end

    def each(& : Client ->) : Nil
      @connections.each { |c| yield c }
    end

    def to_a : Array(Client)
      @connections.dup
    end

    def size : Int32
      @connections.size
    end

    def empty? : Bool
      @connections.empty?
    end

    def close_all(reason : String, log : Logger) : Nil
      WaitGroup.wait do |wg|
        to_close = Channel(Client).new
        fiber_count = 0
        @connections.each do |client|
          select
          when to_close.send client
          else
            fiber_id = fiber_count &+= 1
            log.trace { "spawning close conn fiber #{fiber_id} " }
            client_inner = client
            wg.spawn do
              client_inner.close(reason)
              while client_to_close = to_close.receive?
                client_to_close.close(reason)
              end
              log.trace { "exiting close conn fiber #{fiber_id} " }
            end
            Fiber.yield
          end
        end
        to_close.close
      end
    end
  end
end

require "sync/shared"
require "wait_group"
require "./client/client"
require "./logger"

module LavinMQ
  class ConnectionStore
    @connections : Sync::Shared(Array(Client))

    def initialize
      @connections = Sync::Shared.new(Array(Client).new(512), :unchecked)
    end

    def add(client : Client)
      @connections.lock { |c| c << client }
    end

    def delete(client : Client)
      @connections.lock(&.delete(client))
    end

    def each(& : Client ->) : Nil
      @connections.shared do |conns|
        conns.each { |c| yield c }
      end
    end

    def to_a : Array(Client)
      @connections.shared(&.dup)
    end

    def size : Int32
      @connections.unsafe_get.size
    end

    def empty? : Bool
      @connections.unsafe_get.empty?
    end

    def close_all(reason : String, log : Logger) : Nil
      WaitGroup.wait do |wg|
        to_close = Channel(Client).new
        fiber_count = 0
        @connections.shared(&.dup).each do |client|
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

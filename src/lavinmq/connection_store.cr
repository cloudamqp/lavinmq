require "wait_group"
require "sync/exclusive"
require "./client/client"
require "./logger"

module LavinMQ
  class ConnectionStore
    @connections = Sync::Exclusive(Set(Client)).new(Set(Client).new(512), :unchecked)

    def add(client : Client) : Nil
      @connections.lock &.add(client)
    end

    def delete(client : Client)
      @connections.lock &.delete(client)
    end

    def each(& : Client ->) : Nil
      # A callback can close a client, which removes itself from this store.
      to_a.each { |c| yield c }
    end

    def to_a : Array(Client)
      @connections.lock &.to_a
    end

    def size : Int32
      @connections.lock &.size
    end

    def empty? : Bool
      @connections.lock &.empty?
    end

    def close_all(reason : String, log : Logger) : Nil
      WaitGroup.wait do |wg|
        to_close = Channel(Client).new
        fiber_count = 0
        each do |client|
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

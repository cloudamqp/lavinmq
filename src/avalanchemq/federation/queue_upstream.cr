require "./upstream"

module AvalancheMQ
  class QueueUpstream < Upstream
    property queue

    @queue : String?

    def initialize(vhost : VHost, name : String, uri : String, @queue = nil,
                   prefetch = DEFAULT_PREFETCH, reconnect_delay = DEFUALT_RECONNECT_DELAY,
                   ack_mode = DEFAULT_ACK_MODE)
      super(vhost, name, uri, prefetch.to_u16, reconnect_delay, ack_mode)
    end

    def close_link(federated_q : Queue)
      @links.delete(federated_q.name).try(&.close)
    end

    # When federated_q has a consumer the connections are estabished.
    # If all consumers disconnect, the connections are closed.
    # When the policy or the upstream is removed the link is also removed.
    def link(federated_q : Queue)
      @log.debug "link #{federated_q.name}"
      @queue ||= federated_q.name
      link = Link.new(self, federated_q, @log.dup)
      @links[federated_q.name] = link
      spawn(name: "Upstream #{@uri.host}/#{@queue}") do
        sleep 0.05
        loop do
          unless link.start # blocking
            @log.debug { "Waiting for consumers" }
            sleep @reconnect_delay.seconds
          end
        rescue ex
          break unless @links[federated_q.name]?
          @log.warn "Failure: #{ex.inspect_with_backtrace}"
          sleep @reconnect_delay.seconds
        end
        @log.debug { "Link stopped" }
      end
      @log.info { "Link starting" }
      Fiber.yield
    end
  end
end

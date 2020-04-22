require "./upstream"

module AvalancheMQ
  module Federation
    class QueueUpstream < Upstream
      Log = ::Log.for(self)
      property queue

      @queue : String?

      def initialize(vhost : VHost, name : String, uri : String, @queue = nil,
                     prefetch = DEFAULT_PREFETCH, reconnect_delay = DEFUALT_RECONNECT_DELAY,
                     ack_mode = DEFAULT_ACK_MODE)
        super(vhost, name, uri, prefetch.to_u16, reconnect_delay, ack_mode)
      end

      def stop_link(federated_q : Queue)
        @links.delete(federated_q.name).try(&.stop)
      end

      # When federated_q has a consumer the connections are estabished.
      # If all consumers disconnect, the connections are closed.
      # When the policy or the upstream is removed the link is also removed.
      def link(federated_q : Queue)
        return if @links[federated_q.name]?
        @queue ||= federated_q.name
        link = Link.new(self, federated_q)
        @links[federated_q.name] = link
        link.run
      end
    end
  end
end

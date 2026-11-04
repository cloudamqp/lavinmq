require "../sortable_json"
require "./channel/consumer"

module LavinMQ
  abstract class Client
    abstract class Channel
      include SortableJSON
      @name = ""

      # Called once per Server::IDLE_RELEASE_INTERVAL for every channel. Must be
      # quick and must not wait on locks or sockets: the pass runs every channel
      # of every connection in one fiber. Heavier work belongs with its owner.
      def release_idle_buffers : Nil
      end
    end
  end
end

require "uri"
require "./link"

module AvalancheMQ
  module Federation
    Log = ::Log.for(self)
    enum AckMode
      OnConfirm
      OnPublish
      NoAck
    end

    class Upstream
      Log = ::Log.for(self)
      DEFAULT_PREFETCH        = 1000_u16
      DEFUALT_RECONNECT_DELAY =    1_i32
      DEFAULT_ACK_MODE        = AckMode::OnConfirm
      DEFAULT_MAX_HOPS        = 1
      DEFAULT_EXPIRES         = "none"
      DEFAULT_MSG_TTL         = "none"

      @links = Hash(String, Link).new

      getter name, vhost, links
      property uri, prefetch, reconnect_delay, ack_mode

      def initialize(@vhost : VHost, @name : String, raw_uri : String, @prefetch = DEFAULT_PREFETCH,
                     @reconnect_delay = DEFUALT_RECONNECT_DELAY, @ack_mode = DEFAULT_ACK_MODE)
        @uri = URI.parse(raw_uri)
      end

      def close
        @links.each_value(&.stop)
        @links.clear
      end
    end
  end
end

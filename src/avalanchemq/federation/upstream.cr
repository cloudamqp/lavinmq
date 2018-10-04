require "uri"
require "logger"
require "./link"

module AvalancheMQ
  class Upstream
    DEFAULT_PREFETCH        = 1000_u16
    DEFUALT_RECONNECT_DELAY =    1_i32
    DEFAULT_ACK_MODE        = AckMode::OnConfirm
    DEFAULT_MAX_HOPS        = 1
    DEFAULT_EXPIRES         = "none"
    DEFAULT_MSG_TTL         = "none"

    enum AckMode
      OnConfirm
      OnPublish
      NoAck
    end

    @log : Logger
    @links = Hash(String, Link).new

    getter name, log, vhost, links
    property uri, prefetch, reconnect_delay, ack_mode

    def initialize(@vhost : VHost, @name : String, raw_uri : String, @prefetch = DEFAULT_PREFETCH,
                   @reconnect_delay = DEFUALT_RECONNECT_DELAY, @ack_mode = DEFAULT_ACK_MODE)
      @uri = URI.parse(raw_uri)
      @log = @vhost.log.dup
      @log.progname += " upstream=#{@name}"
    end

    def close
      @links.values.each(&.stop)
      @links.clear
    end
  end
end

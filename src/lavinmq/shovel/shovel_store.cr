require "./shovel"

module LavinMQ
  class ShovelStore
    def initialize(@vhost : VHost)
      @shovels = Hash(String, Shovel::Runner).new
    end

    forward_missing_to @shovels

    def parse_uris(src_uri : JSON::Any) : Array(URI)
      uris = src_uri.as_s? ? [src_uri.as_s] : src_uri.as_a.map(&.as_s)
      uris.map do |uri|
        URI.parse(uri)
      end
    end

    def upsert(name, config)
      shovel = @shovels[name]?.try { |s| update_status_or_terminate(s, config) }
      shovel ||= create(name, config)
      shovel
    end

    private def update_status_or_terminate(shovel, config)
      case {shovel.state, config["state"]?.try &.as_s}
      when {"Running", "Paused"}
        shovel.pause
        return shovel
      when {"Paused", "Running"}
        spawn(shovel.resume, name: "Shovel name=#{shovel.name} vhost=#{@vhost.name}")
        return shovel
      end
      shovel.terminate
      nil
    end

    def delete(name)
      if shovel = @shovels.delete name
        shovel.terminate
        shovel
      end
    end

    private def create(name, config)
      delete_after_str = config["src-delete-after"]?.try(&.as_s.delete("-")).to_s
      delete_after = Shovel::DeleteAfter.parse?(delete_after_str) || Shovel::DEFAULT_DELETE_AFTER
      ack_mode_str = config["ack-mode"]?.try(&.as_s.delete("-")).to_s
      ack_mode = Shovel::AckMode.parse?(ack_mode_str) || Shovel::DEFAULT_ACK_MODE
      reconnect_delay = config["reconnect-delay"]?.try &.as_i.seconds || Shovel::DEFAULT_RECONNECT_DELAY
      prefetch = config["src-prefetch-count"]?.try(&.as_i.to_u16) || Shovel::DEFAULT_PREFETCH
      src = Shovel::AMQPSource.new(name, parse_uris(config["src-uri"]),
        config["src-queue"]?.try &.as_s?,
        config["src-exchange"]?.try &.as_s?,
        config["src-exchange-key"]?.try &.as_s?,
        delete_after,
        prefetch,
        ack_mode,
        config["src-consumer-args"]?.try &.as_h?,
        direct_user: @vhost.users.direct_user)
      dest = destination(name, config, ack_mode)
      shovel = Shovel::Runner.new(src, dest, name, @vhost, reconnect_delay)
      @shovels[name] = shovel
      spawn(shovel.run, name: "Shovel name=#{name} vhost=#{@vhost.name}")
      shovel
    rescue KeyError
      raise JSON::Error.new("Fields 'src-uri' and 'dest-uri' are required")
    end

    private def destination(name, config, ack_mode)
      uris = parse_uris(config["dest-uri"])
      destinations = uris.map do |uri|
        case uri.scheme
        when "http", "https"
          Shovel::HTTPDestination.new(name, uri)
        else
          Shovel::AMQPDestination.new(name, uri,
            config["dest-queue"]?.try &.as_s?,
            config["dest-exchange"]?.try &.as_s?,
            config["dest-exchange-key"]?.try &.as_s?,
            ack_mode,
            direct_user: @vhost.users.direct_user)
        end
      end
      Shovel::MultiDestinationHandler.new(destinations)
    end
  end
end

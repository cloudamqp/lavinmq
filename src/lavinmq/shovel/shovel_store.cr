require "./shovel"

module LavinMQ
  class ShovelStore
    def initialize(@vhost : VHost)
      @shovels = Hash(String, Shovel::Runner).new
    end

    forward_missing_to @shovels

    def create(name, config)
      @shovels[name]?.try &.terminate
      delete_after_str = config["src-delete-after"]?.try(&.as_s.delete("-")).to_s
      delete_after = Shovel::DeleteAfter.parse?(delete_after_str) || Shovel::DEFAULT_DELETE_AFTER
      ack_mode_str = config["ack-mode"]?.try(&.as_s.delete("-")).to_s
      ack_mode = Shovel::AckMode.parse?(ack_mode_str) || Shovel::DEFAULT_ACK_MODE
      reconnect_delay = config["reconnect-delay"]?.try &.as_i || Shovel::DEFAULT_RECONNECT_DELAY
      prefetch = config["src-prefetch-count"]?.try(&.as_i.to_u16) || Shovel::DEFAULT_PREFETCH
      src = Shovel::AMQPSource.new(name, URI.parse(config["src-uri"].as_s),
        config["src-queue"]?.try &.as_s?,
        config["src-exchange"]?.try &.as_s?,
        config["src-exchange-key"]?.try &.as_s?,
        delete_after,
        prefetch,
        ack_mode,
        direct_user: @vhost.users.direct_user)
      dest = destination(name, config, ack_mode, delete_after, prefetch)
      shovel = Shovel::Runner.new(src, dest, name, @vhost, reconnect_delay)
      @shovels[name] = shovel
      spawn(shovel.run, name: "Shovel name=#{name} vhost=#{@vhost.name}")
      shovel
    rescue KeyError
      raise JSON::Error.new("Fields 'src-uri' and 'dest-uri' are required")
    end

    def delete(name)
      if shovel = @shovels.delete name
        shovel.terminate
        shovel
      end
    end

    private def destination(name, config, ack_mode, delete_after, prefetch)
      uri = URI.parse(config["dest-uri"].as_s)
      case uri.scheme
      when "http", "https"
        Shovel::HTTPDestination.new(name, uri)
      else
        Shovel::AMQPDestination.new(name, uri,
          config["dest-queue"]?.try &.as_s?,
          config["dest-exchange"]?.try &.as_s?,
          config["dest-exchange-key"]?.try &.as_s?,
          delete_after: delete_after,
          prefetch: prefetch,
          direct_user: @vhost.users.direct_user)
      end
    end
  end
end

require "./shovel"
require "../auth/user"

module LavinMQ
  class ShovelStore
    class Error < Exception; end

    class ConfigError < Error; end

    def initialize(@vhost : VHost)
      @shovels = Hash(String, Shovel::Runner).new
    end

    forward_missing_to @shovels

    # ameba:disable Metrics/CyclomaticComplexity
    def self.validate_config!(config : JSON::Any, user : Auth::User?)
      dest_uris = parse_uris(config["dest-uri"]?)
      src_uris = parse_uris(config["src-uri"]?)

      src_q = config["src-queue"]?.try(&.as_s)
      src_x = config["src-exchange"]?.try(&.as_s)
      dst = config["dest-exchange"]?.try(&.as_s)
      dst_q = config["dest-queue"]?.try(&.as_s)

      if dst.nil? && dst_q
        dst = "" # default exchange
      end

      raise ConfigError.new("Shovel source requires a queue or an exchange") if src_q.nil? && src_x.nil?
      raise ConfigError.new("Shovel destination requires queue and/or exchange") if dst.nil?

      return unless user

      dest_uris.select!(&.scheme.try &.starts_with?("amqp"))
      dest_uris.select!(&.host.to_s.empty?)
      dest_uris.select!(&.user.nil?)

      src_uris.select!(&.scheme.try &.starts_with?("amqp"))
      src_uris.select!(&.host.to_s.empty?)
      src_uris.select!(&.user.nil?)

      dest_uris.each do |uri|
        vhost = uri.path
        vhost = "/" if vhost.empty?
        if d = dst
          if !(user.can_write?(vhost, d) && user.can_config?(vhost, d))
            raise ConfigError.new("#{user.name} can't access exchange '#{d}' in #{vhost}")
          end
        end
        if q = dst_q
          if !user.can_config?(vhost, q)
            raise ConfigError.new("#{user.name} can't access queue '#{q}' in #{vhost}")
          end
        end
      end

      src_uris.each do |uri|
        vhost = uri.path
        vhost = "/" if vhost.empty?
        if q = src_q
          if !(user.can_read?(vhost, q) && user.can_config?(vhost, q))
            raise ConfigError.new("#{user.name} can't access queue '#{q}' in #{vhost}")
          end
        end
        if x = src_x
          if !(user.can_read?(vhost, x) && user.can_config?(vhost, x))
            raise ConfigError.new("#{user.name} can't access exchange '#{x}' in #{vhost}")
          end
        end
      end
    end

    def self.parse_uris(src_uri : JSON::Any?) : Array(URI)
      return Array(URI).new if src_uri.nil?
      uris = src_uri.as_s? ? [src_uri.as_s] : src_uri.as_a.map(&.as_s)
      uris.map do |uri|
        URI.parse(uri)
      end
    end

    def create(name, config)
      @shovels[name]?.try &.terminate
      delete_after_str = config["src-delete-after"]?.try(&.as_s.delete("-")).to_s
      delete_after = Shovel::DeleteAfter.parse?(delete_after_str) || Shovel::DEFAULT_DELETE_AFTER
      ack_mode_str = config["ack-mode"]?.try(&.as_s.delete("-")).to_s
      ack_mode = Shovel::AckMode.parse?(ack_mode_str) || Shovel::DEFAULT_ACK_MODE
      reconnect_delay = config["reconnect-delay"]?.try &.as_i.seconds || Shovel::DEFAULT_RECONNECT_DELAY
      prefetch = config["src-prefetch-count"]?.try(&.as_i.to_u16) || Shovel::DEFAULT_PREFETCH
      src = Shovel::AMQPSource.new(name, self.class.parse_uris(config["src-uri"]),
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

    def delete(name)
      if shovel = @shovels.delete name
        shovel.delete
        shovel
      end
    end

    private def destination(name, config, ack_mode)
      uris = self.class.parse_uris(config["dest-uri"])
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

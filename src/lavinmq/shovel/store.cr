require "./runner"
require "./amqp_source"
require "./http_destination.cr"
require "./amqp_destination.cr"
require "./multi_destination.cr"
require "../auth/user"

module LavinMQ
  module Shovel
    class Error < Exception; end

    class ConfigError < Error; end

    class Store
      # The user who saved the shovel, stored in its value. A local source
      # binds as the direct user, so this user's permissions are checked
      # before it binds, see #mqtt_source_guard
      CREATED_BY = "created-by"

      def initialize(@vhost : VHost, @vhost_store : VHostStore? = nil)
        @shovels = Hash(String, Shovel::Runner).new
      end

      def []?(name : String) : Runner?
        @shovels[name]?
      end

      def [](name : String) : Runner
        @shovels[name]
      end

      def each_value(& : Runner ->) : Nil
        @shovels.each_value { |r| yield r }
      end

      def values : Array(Runner)
        @shovels.values
      end

      def size : Int32
        @shovels.size
      end

      def empty? : Bool
        @shovels.empty?
      end

      def has_key?(name : String) : Bool
        @shovels.has_key?(name)
      end

      # ameba:disable Metrics/CyclomaticComplexity
      def self.validate_config!(config : JSON::Any, user : Auth::BaseUser?, vhosts : VHostStore?)
        dest_uris = parse_uris(config["dest-uri"]?)
        src_uris = parse_uris(config["src-uri"]?)

        src_q = config["src-queue"]?.try(&.as_s)
        src_x = config["src-exchange"]?.try(&.as_s)
        dst = config["dest-exchange"]?.try(&.as_s)
        dst_q = config["dest-queue"]?.try(&.as_s)

        if dst.nil? && dst_q
          dst = "" # default exchange
        end

        # HTTP(S) destinations POST to a URL and have no queue/exchange.
        http_dest = !dest_uris.empty? && dest_uris.all?(&.scheme.in?("http", "https"))

        raise ConfigError.new("Shovel source requires a queue or an exchange") if src_q.nil? && src_x.nil?
        raise ConfigError.new("Shovel destination requires queue and/or exchange") if dst.nil? && !http_dest
        validate_dest_timeout!(config["dest-timeout"]?)

        return unless user

        dest_uris.select!(&.scheme.try &.starts_with?("amqp"))
        dest_uris.select!(&.host.to_s.empty?)
        dest_uris.select!(&.user.nil?)

        src_uris.select!(&.scheme.try &.starts_with?("amqp"))
        src_uris.select!(&.host.to_s.empty?)
        src_uris.select!(&.user.nil?)

        dest_uris.each do |uri|
          vhost = vhost_from_uri(uri)
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
          vhost = vhost_from_uri(uri)
          if q = src_q
            if !(user.can_read?(vhost, q) && user.can_config?(vhost, q))
              raise ConfigError.new("#{user.name} can't access queue '#{q}' in #{vhost}")
            end
          end
          if x = src_x
            if !(user.can_read?(vhost, x) && user.can_config?(vhost, x))
              raise ConfigError.new("#{user.name} can't access exchange '#{x}' in #{vhost}")
            end
            validate_mqtt_source!(user, vhosts.try(&.[vhost]?), x)
          end
        end
      end

      # Refuses early a shovel from an existing x-mqtt-topic exchange. The
      # exchange can be declared later, by any user allowed to, and the read
      # can be revoked, so #mqtt_source_guard checks again on every bind
      private def self.validate_mqtt_source!(user : Auth::BaseUser, vhost : VHost?, exchange_name : String)
        return unless vhost
        return unless vhost.exchange?(exchange_name).is_a?(AMQP::MqttTopicExchange)
        return if AMQP::MqttTopicExchange.readable_by?(user, vhost.name)
        raise ConfigError.new(AMQP::MqttTopicExchange.unreadable_message(user, exchange_name))
      end

      # A malformed dest-timeout fails the PUT like any other bad field, rather
      # than being stored and silently replaced by the default at start.
      private def self.validate_dest_timeout!(value : JSON::Any?)
        return if value.nil?
        secs = value.as_f? || value.as_i?.try(&.to_f)
        return if secs && secs > 0
        raise ConfigError.new("dest-timeout must be a positive number of seconds")
      end

      def self.vhost_from_uri(uri : URI) : String
        path = URI.decode(uri.path.lchop("/"))
        path.empty? ? "/" : path
      end

      def self.parse_uris(src_uri : JSON::Any?) : Array(URI)
        return Array(URI).new if src_uri.nil?
        uris = src_uri.as_s? ? [src_uri.as_s] : src_uri.as_a.map(&.as_s)
        uris.map do |uri|
          URI.parse(uri)
        end
      end

      def create(name, config)
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
          direct_user: @vhost.users.direct_user,
          bind_guard: mqtt_source_guard(config[CREATED_BY]?.try &.as_s?))
        dest = destination(name, config, ack_mode)
        shovel = Shovel::Runner.new(src, dest, name, @vhost, reconnect_delay)
        # Terminated only after the new config parsed, so a rejected update
        # leaves the old shovel running
        @shovels[name]?.try &.terminate
        @shovels[name] = shovel
        spawn(shovel.run, name: "Shovel name=#{name} vhost=#{@vhost.name}")
        shovel
      rescue KeyError
        raise JSON::Error.new("Fields 'src-uri' and 'dest-uri' are required")
      end

      # A local source binds as the direct user, which may read everything.
      # Before each bind, on every start, resume and reconnect, check that the
      # shovel's creator may read an x-mqtt-topic source. A refusal raises,
      # which leaves the shovel in its error state, retrying. Shovels saved
      # without a creator, before this check or by a definitions import, are
      # not checked
      private def mqtt_source_guard(created_by : String?) : AMQPSource::BindGuard?
        # Assigned once, so the closure sees a String, not the String? argument
        owner = created_by || return
        vhost_store = @vhost_store
        users = @vhost.users
        ->(vhost_name : String, exchange_name : String) do
          # The vhost can be missing at boot, while vhosts are still loading
          vhost = vhost_store.try &.[vhost_name]?
          raise Error.new("Vhost '#{vhost_name}' not found") unless vhost
          return unless vhost.exchange?(exchange_name).is_a?(AMQP::MqttTopicExchange)
          user = users[owner]?
          raise Error.new("User '#{owner}', who created the shovel, doesn't exist") unless user
          return if AMQP::MqttTopicExchange.readable_by?(user, vhost_name)
          raise Error.new(AMQP::MqttTopicExchange.unreadable_message(user, exchange_name))
        end
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
            Shovel::HTTPDestination.new(name, uri, ack_mode, Shovel::HTTPDestination.timeout_from(config))
          else
            Shovel::AMQPDestination.new(name, uri,
              config["dest-queue"]?.try &.as_s?,
              config["dest-exchange"]?.try &.as_s?,
              config["dest-exchange-key"]?.try &.as_s?,
              ack_mode,
              direct_user: @vhost.users.direct_user)
          end
        end
        Shovel::MultiDestination.new(destinations)
      end
    end
  end
end

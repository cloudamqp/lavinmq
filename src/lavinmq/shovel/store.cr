require "./runner"
require "./amqp_source"
require "./postgres_source"
require "./http_destination"
require "./amqp_destination"
require "./multi_destination"
require "../endpoint"
require "../auth/base_user"

module LavinMQ
  module Shovel
    alias ConfigError = Endpoint::ConfigError

    class Store
      def initialize(@vhost : VHost)
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
      def self.validate_config!(config : JSON::Any, user : Auth::BaseUser?)
        dest_uris = Endpoint.parse_uris(config["dest-uri"]?)
        src_uris = Endpoint.parse_uris(config["src-uri"]?)

        src_q = config["src-queue"]?.try(&.as_s)
        src_x = config["src-exchange"]?.try(&.as_s)
        dst = config["dest-exchange"]?.try(&.as_s)
        dst_q = config["dest-queue"]?.try(&.as_s)

        if dst.nil? && dst_q
          dst = "" # default exchange
        end

        # HTTP(S) destinations POST to a URL and have no queue/exchange.
        http_dest = !dest_uris.empty? && dest_uris.all?(&.scheme.in?("http", "https"))

        if postgres_source?(src_uris)
          validate_postgres_source!(config, src_uris)
        elsif src_q.nil? && src_x.nil?
          raise ConfigError.new("Shovel source requires a queue or an exchange")
        end
        raise ConfigError.new("Shovel destination requires queue and/or exchange") if dst.nil? && !http_dest
        validate_dest_timeout!(config["dest-timeout"]?)

        return unless user

        # In-process endpoints act with no user of their own, so the user
        # creating the shovel must be allowed what the shovel will do there.
        # A remote endpoint is authorized by its own broker, with the URI's
        # credentials.
        dest_uris.select! { |uri| Endpoint.local?(uri) }
        src_uris.select! { |uri| Endpoint.local?(uri) }

        dest_uris.each do |uri|
          vhost = Endpoint.vhost_name(uri)
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
          vhost = Endpoint.vhost_name(uri)
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

      # A malformed dest-timeout fails the PUT like any other bad field, rather
      # than being stored and silently replaced by the default at start.
      private def self.validate_dest_timeout!(value : JSON::Any?)
        return if value.nil?
        secs = value.as_f? || value.as_i?.try(&.to_f)
        return if secs && secs > 0
        raise ConfigError.new("dest-timeout must be a positive number of seconds")
      end

      def self.postgres_source?(src_uris : Array(URI)) : Bool
        src_uris.any?(&.scheme.in?("postgres", "postgresql"))
      end

      SLOT_NAME = /\A[a-z0-9_]{1,63}\z/

      private def self.validate_postgres_source!(config : JSON::Any, src_uris : Array(URI))
        raise ConfigError.new("A PostgreSQL shovel takes exactly one src-uri") unless src_uris.size == 1
        begin
          Postgres::Connection.sslmode(src_uris.first)
        rescue ex : Postgres::Error
          raise ConfigError.new(ex.message)
        end
        unless config["src-publication"]?.try(&.as_s?).presence
          raise ConfigError.new("A PostgreSQL shovel source requires src-publication")
        end
        if slot = config["src-slot"]?
          unless slot.as_s?.try &.matches?(SLOT_NAME)
            raise ConfigError.new("src-slot may only contain lower case letters, numbers and underscores (max 63)")
          end
        end
        delete_after = config["src-delete-after"]?.try(&.as_s?).to_s.delete("-")
        if DeleteAfter.parse?(delete_after).try &.queue_length?
          raise ConfigError.new("src-delete-after: queue-length is not supported for a PostgreSQL source")
        end
        if src_uris.first.user.to_s.empty?
          raise ConfigError.new("The PostgreSQL src-uri requires a user")
        end
      end

      def create(name, config)
        @shovels[name]?.try &.terminate
        delete_after_str = config["src-delete-after"]?.try(&.as_s.delete("-")).to_s
        delete_after = Shovel::DeleteAfter.parse?(delete_after_str) || Shovel::DEFAULT_DELETE_AFTER
        ack_mode = AckMode.from_config?(config["ack-mode"]?.try(&.as_s)) || Shovel::DEFAULT_ACK_MODE
        reconnect_delay = config["reconnect-delay"]?.try &.as_i.seconds || Shovel::DEFAULT_RECONNECT_DELAY
        prefetch = config["src-prefetch-count"]?.try(&.as_i.to_u16) || Shovel::DEFAULT_PREFETCH
        src = source(name, config, delete_after, prefetch, ack_mode)
        dest = destination(name, config, ack_mode)
        shovel = Shovel::Runner.new(src, dest, name, @vhost, reconnect_delay)
        @shovels[name] = shovel
        # A shovel restored paused is started by #resume. A run spawned for it
        # here could start only after a resume, and run alongside its run.
        unless shovel.paused?
          spawn(shovel.run, name: "Shovel name=#{name} vhost=#{@vhost.name}")
        end
        shovel
      rescue KeyError
        raise JSON::Error.new("Fields 'src-uri' and 'dest-uri' are required")
      end

      # Consumer arguments, e.g. `{"x-stream-offset": "first"}`. Strings,
      # integers and booleans are passed on, anything else is ignored.
      def self.consumer_args(value : JSON::Any?) : AMQ::Protocol::Table
        args = AMQ::Protocol::Table.new
        hash = value.try(&.as_h?) || return args
        hash.each do |k, v|
          case raw = v.raw
          when String, Int64, Bool then args[k] = raw
          end
        end
        args
      end

      def delete(name)
        if shovel = @shovels.delete name
          shovel.delete
          shovel
        end
      end

      private def source(name, config, delete_after, prefetch, ack_mode) : Source
        uris = Endpoint.parse_uris(config["src-uri"])
        if self.class.postgres_source?(uris)
          Shovel::PostgresSource.new(name, uris.first,
            config["src-publication"].as_s,
            config["src-slot"]?.try(&.as_s?) || Shovel::PostgresSource.default_slot_name(@vhost.name, name),
            prefetch,
            ack_mode)
        else
          sessions = uris.map do |uri|
            Endpoint.session(uri, @vhost, "Shovel #{name} source")
          end
          Shovel::AMQPSource.new(name, sessions,
            config["src-queue"]?.try &.as_s?,
            config["src-exchange"]?.try &.as_s?,
            config["src-exchange-key"]?.try &.as_s?,
            delete_after,
            prefetch,
            ack_mode,
            self.class.consumer_args(config["src-consumer-args"]?))
        end
      end

      private def destination(name, config, ack_mode)
        uris = Endpoint.parse_uris(config["dest-uri"])
        destinations = uris.map do |uri|
          case uri.scheme
          when "http", "https"
            Shovel::HTTPDestination.new(name, uri, ack_mode, Shovel::HTTPDestination.timeout_from(config))
          else
            Shovel::AMQPDestination.new(name,
              Endpoint.session(uri, @vhost, "Shovel #{name} sink"),
              config["dest-queue"]?.try &.as_s?,
              config["dest-exchange"]?.try &.as_s?,
              config["dest-exchange-key"]?.try &.as_s?,
              ack_mode)
          end
        end
        Shovel::MultiDestination.new(destinations)
      end
    end
  end
end

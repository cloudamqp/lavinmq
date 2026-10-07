require "./upstream"
require "../logger"
require "../endpoint"
require "../auth/base_user"

module LavinMQ
  module Federation
    class ConfigError < Exception; end

    class UpstreamStore
      include Enumerable(Upstream)
      Log = LavinMQ::Log.for "federation.upstream_store"
      @upstreams = Hash(String, Upstream).new
      @upstream_sets = Hash(String, Array(Upstream)).new

      def initialize(@vhost : VHost)
        @metadata = ::Log::Metadata.new(nil, {vhost: @vhost.name})
        @log = Logger.new(Log, @metadata)
      end

      # An upstream in this broker (a URI without host) is reached in-process,
      # with no user of its own, so the user configuring it must have access
      # to it. Which resources a link uses there isn't known here: without an
      # `exchange` or `queue` the downstream resource's name is used, as
      # chosen by policy later, and an exchange link declares a queue and
      # exchange of its own. So the user needs unrestricted read and
      # configure permission on the upstream vhost. A remote upstream is
      # authorized by its broker, with the URI's credentials.
      def self.validate_config!(component : String, config : JSON::Any, user : Auth::BaseUser?)
        entries = case component
                  when "federation-upstream"     then [config]
                  when "federation-upstream-set" then config.as_a? || raise ConfigError.new("Upstream set must be an array")
                  else                                return
                  end
        entries.each do |entry|
          uris = parse_uris(entry["uri"]?)
          if component == "federation-upstream" && uris.empty?
            raise ConfigError.new("Field 'uri' is required")
          end
          uris.each { |uri| validate_access!(URI.parse(uri), entry, user) } if user
        end
      end

      # A URI or, as RabbitMQ accepts, a list of them. Only the first is
      # used, an upstream connects to one broker.
      def self.parse_uris(value : JSON::Any?) : Array(String)
        return Array(String).new if value.nil?
        if uri = value.as_s?
          [uri]
        elsif list = value.as_a?
          list.map { |v| v.as_s? || raise ConfigError.new("Field 'uri' must be a string or a list of strings") }
        else
          raise ConfigError.new("Field 'uri' must be a string or a list of strings")
        end
      end

      private def self.validate_access!(uri : URI, entry : JSON::Any, user : Auth::BaseUser)
        return unless Endpoint.local?(uri)
        vhost = Endpoint.vhost_name(uri)
        perm = user.find_permission(vhost)
        unless perm && unrestricted?(perm[:read]) && unrestricted?(perm[:config])
          raise ConfigError.new("#{user.name} needs read and configure permission on everything in vhost '#{vhost}'")
        end
      end

      private def self.unrestricted?(pattern : Regex) : Bool
        pattern.source.in?(".*", "^.*$", "^.*", ".*$")
      end

      def each(&)
        @upstreams.each_value do |v|
          yield v
        end
      end

      def create_upstream(name, config)
        do_delete_upstream(name)
        uri = self.class.parse_uris(config["uri"]?).first? || raise ConfigError.new("Field 'uri' is required")
        prefetch = config["prefetch-count"]?.try(&.as_i.to_u16) || DEFAULT_PREFETCH
        reconnect_delay = config["reconnect-delay"]?.try(&.as_i?).try &.seconds || DEFAULT_RECONNECT_DELAY
        ack_mode = AckMode.from_config?(config["ack-mode"]?.try(&.as_s)) || DEFAULT_ACK_MODE
        exchange = config["exchange"]?.try(&.as_s)
        max_hops = config["max-hops"]?.try(&.as_i64?) || DEFAULT_MAX_HOPS
        expires = config["expires"]?.try(&.as_i64?) || DEFAULT_EXPIRES
        msg_ttl = config["message-ttl"]?.try(&.as_i64?) || DEFAULT_MSG_TTL
        consumer_tag = config["consumer-tag"]?.try(&.as_s?) || "federation-link-#{name}"
        # trust_user_id
        queue = config["queue"]?.try(&.as_s)
        @upstreams[name] = Upstream.new(@vhost, name, uri, exchange, queue, ack_mode, expires,
          max_hops, msg_ttl, prefetch, reconnect_delay, consumer_tag)
        @log.info { "Upstream '#{name}' created" }
        @upstreams[name]
      end

      def add(upstream : Upstream)
        @upstreams[upstream.name]?.try &.close
        @upstreams[upstream.name] = upstream
      end

      def delete_upstream(name)
        do_delete_upstream(name)
        @log.info { "Upstream '#{name}' deleted" }
      end

      private def do_delete_upstream(name)
        @upstreams.delete(name).try(&.delete)
        @upstream_sets.each do |_, set|
          set.reject! do |upstream|
            next false unless upstream.name == name
            upstream.delete
            true
          end
        end
      end

      def link(name, resource : AMQP::Queue | Exchange)
        @upstreams[name]?.try &.link(resource)
      end

      def stop_link(resource : AMQP::Queue | Exchange)
        each_linked_upstream do |upstream|
          upstream.stop_link(resource)
        end
      end

      def exchange_bound(exchange : AMQP::Exchange, binding : AMQP::BindingDetails)
        each_linked_upstream do |upstream|
          upstream.exchange_bound(exchange, binding)
        end
      end

      def exchange_unbound(exchange : AMQP::Exchange, binding : AMQP::BindingDetails)
        each_linked_upstream do |upstream|
          upstream.exchange_unbound(exchange, binding)
        end
      end

      # Every upstream that can own links: the named upstreams plus the copies
      # upstream sets make when an entry overrides settings (see Upstream#dup)
      private def each_linked_upstream(&)
        @upstreams.each_value { |upstream| yield upstream }
        @upstream_sets.each_value do |set|
          set.each do |upstream|
            yield upstream if set_copy?(upstream)
          end
        end
      end

      # A set entry with overrides is a copy owning its own links, the others
      # are the named upstream itself
      private def set_copy?(upstream : Upstream) : Bool
        !@upstreams[upstream.name]?.same?(upstream)
      end

      def create_upstream_set(name, config)
        # Re-applied policies link the new set; keep the upstream resources
        # for those links to reuse
        @upstream_sets.delete(name).try &.each { |u| u.close if set_copy?(u) }
        upstreams = Array(Upstream).new
        config.as_a.each do |cfg|
          upstream = @upstreams[cfg["upstream"].as_s]
          if cfg.as_h.keys.size > 1
            upstream = upstream.dup
            self.class.parse_uris(cfg["uri"]?).first?.try { |p| upstream.uri = URI.parse(p) }
            cfg["prefetch-count"]?.try { |p| upstream.prefetch = p.as_i.to_u16 }
            cfg["reconnect-delay"]?.try { |p| upstream.reconnect_delay = p.as_i.seconds }
            AckMode.from_config?(cfg["ack-mode"]?.try(&.as_s)).try { |p| upstream.ack_mode = p }
            cfg["exchange"]?.try { |p| upstream.exchange = p.as_s }
            cfg["max-hops"]?.try { |p| upstream.max_hops = p.as_i64 }
            cfg["expires"]?.try { |p| upstream.expires = p.as_i64 }
            cfg["message-ttl"]?.try { |p| upstream.msg_ttl = p.as_i64 }
            cfg["queue"]?.try { |p| upstream.queue = p.as_s }
          end
          upstreams << upstream
        end
        @upstream_sets[name] = upstreams
      end

      def delete_upstream_set(name)
        @upstream_sets.delete(name).try &.each { |u| u.delete if set_copy?(u) }
        @log.info { "Upstream set '#{name}' deleted" }
      end

      def link_set(name, resource : Exchange | Queue)
        set = get_set(name)
        set.each do |upstream|
          upstream.link(resource)
        end
      end

      def get_set(name)
        case name
        when "all"
          @upstreams.values
        else
          @upstream_sets[name]
        end
      end

      def stop_all
        @upstreams.each_value &.close
        @upstream_sets.values.flatten.each &.close
      end
    end
  end
end

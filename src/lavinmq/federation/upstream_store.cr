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
      # to it: to the vhost, and read and configure permission on the
      # configured upstream exchange and queue. A remote upstream is
      # authorized by its broker, with the URI's credentials.
      def self.validate_config!(component : String, config : JSON::Any, user : Auth::BaseUser?)
        entries = case component
                  when "federation-upstream"     then [config]
                  when "federation-upstream-set" then config.as_a? || raise ConfigError.new("Upstream set must be an array")
                  else                                return
                  end
        entries.each do |entry|
          uri_str = entry["uri"]?.try(&.as_s?)
          if component == "federation-upstream" && uri_str.nil?
            raise ConfigError.new("Field 'uri' is required")
          end
          next unless uri_str && user
          uri = URI.parse(uri_str)
          next unless Endpoint.local?(uri)
          vhost = Endpoint.vhost_name(uri)
          unless user.find_permission(vhost)
            raise ConfigError.new("#{user.name} can't access vhost '#{vhost}'")
          end
          {entry["exchange"]?, entry["queue"]?}.each do |resource|
            name = resource.try(&.as_s?) || next
            next if name.empty?
            unless user.can_read?(vhost, name) && user.can_config?(vhost, name)
              raise ConfigError.new("#{user.name} can't access '#{name}' in vhost '#{vhost}'")
            end
          end
        end
      end

      def each(&)
        @upstreams.each_value do |v|
          yield v
        end
      end

      def create_upstream(name, config)
        do_delete_upstream(name)
        uri = config["uri"].to_s
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
        each do |upstream|
          upstream.stop_link(resource)
        end
      end

      def create_upstream_set(name, config)
        @upstream_sets.delete(name)
        upstreams = Array(Upstream).new
        config.as_a.each do |cfg|
          upstream = @upstreams[cfg["upstream"].as_s]
          if cfg.as_h.keys.size > 1
            upstream = upstream.dup
            cfg["uri"]?.try { |p| upstream.uri = URI.parse(p.as_s) }
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
        @upstream_sets.delete(name)
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

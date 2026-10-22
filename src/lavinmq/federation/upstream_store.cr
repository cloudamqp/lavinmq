require "./upstream"
require "../logger"

module LavinMQ
  module Federation
    class UpstreamStore
      include Enumerable(Upstream)
      Log = LavinMQ::Log.for "federation.upstream_store"
      @upstreams = Hash(String, Upstream).new
      @upstream_sets = Hash(String, Array(Upstream)).new
      @upstream_set_configs = Hash(String, JSON::Any).new

      def initialize(@vhost : VHost)
        @metadata = ::Log::Metadata.new(nil, {vhost: @vhost.name})
        @log = Logger.new(Log, @metadata)
      end

      def each(&)
        @upstreams.each_value do |v|
          yield v
        end
      end

      def create_upstream(name, config)
        uri = config["uri"].to_s
        prefetch = config["prefetch-count"]?.try(&.as_i.to_u16) || DEFAULT_PREFETCH
        reconnect_delay = config["reconnect-delay"]?.try(&.as_i?).try &.seconds || DEFAULT_RECONNECT_DELAY
        ack_mode_str = config["ack-mode"]?.try(&.as_s.delete("-")).to_s
        ack_mode = AckMode.parse?(ack_mode_str) || DEFAULT_ACK_MODE
        exchange = config["exchange"]?.try(&.as_s)
        max_hops = config["max-hops"]?.try(&.as_i64?) || DEFAULT_MAX_HOPS
        expires = config["expires"]?.try(&.as_i64?) || DEFAULT_EXPIRES
        msg_ttl = config["message-ttl"]?.try(&.as_i64?) || DEFAULT_MSG_TTL
        consumer_tag = config["consumer-tag"]?.try(&.as_s?) || "federation-link-#{name}"
        # trust_user_id
        queue = config["queue"]?.try(&.as_s)
        old = @upstreams[name]?
        if old && same_resources?(old, uri, exchange, queue, expires, msg_ttl, max_hops)
          close_upstream(name)
        else
          do_delete_upstream(name)
        end
        @upstreams[name] = Upstream.new(@vhost, name, uri, exchange, queue, ack_mode, expires,
          max_hops, msg_ttl, prefetch, reconnect_delay, consumer_tag)
        rebuild_sets_with(name)
        @log.info { "Upstream '#{name}' created" }
        @upstreams[name]
      end

      # Replacing an upstream drops its entries from every set; rebuild the
      # sets that name it so a policy re-apply can link through them again
      private def rebuild_sets_with(name)
        @upstream_set_configs.each do |set_name, config|
          next unless config.as_a.any? { |cfg| cfg["upstream"].as_s == name }
          create_upstream_set(set_name, config)
        end
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

      # Whether the resources on the upstream broker can be reused: the
      # settings that name them, plus the declare arguments that a passive
      # re-declare would silently ignore
      private def same_resources?(old : Upstream, uri, exchange, queue, expires, msg_ttl, max_hops) : Bool
        old.uri == URI.parse(uri) && old.exchange == exchange && old.queue == queue &&
          old.expires == expires && old.msg_ttl == msg_ttl && old.max_hops == max_hops
      end

      # Remove the upstream like do_delete_upstream, but keep the resources it
      # created on the upstream broker: a replacement upstream's links reuse
      # them, so their buffered messages survive a parameter re-PUT
      private def close_upstream(name)
        @upstreams.delete(name).try(&.close)
        @upstream_sets.each do |_, set|
          set.reject! do |upstream|
            next false unless upstream.name == name
            upstream.close
            true
          end
        end
      end

      # Make the resource's links match its policy after a policy (re)apply.
      # A link whose upstream the policy still names is kept untouched: that
      # keeps the upstream queue, and its buffered messages, alive. The other
      # links are stopped and their upstream resources removed.
      def reconcile_link(resource : AMQP::Queue | Exchange, upstream_name : String?, set_name : String?)
        desired = Array(Upstream).new
        if name = upstream_name
          if upstream = @upstreams[name]?
            desired << upstream
          else
            @log.warn { "Federation upstream '#{name}' not found" }
          end
        end
        if name = set_name
          if set = get_set?(name)
            desired.concat set
          else
            @log.warn { "Federation upstream set '#{name}' not found" }
          end
        end
        each_linked_upstream do |linked|
          linked.stop_link(resource) unless desired.any? &.same?(linked)
        end
        desired.each &.link(resource)
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
        @upstream_set_configs[name] = config
        upstreams = Array(Upstream).new
        config.as_a.each do |cfg|
          unless upstream = @upstreams[cfg["upstream"].as_s]?
            @log.warn { "Federation upstream '#{cfg["upstream"]}' not found for set '#{name}'" }
            next
          end
          if cfg.as_h.keys.size > 1
            upstream = upstream.dup
            cfg["uri"]?.try { |p| upstream.uri = URI.parse(p.as_s) }
            cfg["prefetch-count"]?.try { |p| upstream.prefetch = p.as_i.to_u16 }
            cfg["reconnect-delay"]?.try { |p| upstream.reconnect_delay = p.as_i.seconds }
            ack_mode_str = cfg["ack-mode"]?.try(&.as_s.delete("-")).to_s
            AckMode.parse?(ack_mode_str).try { |p| upstream.ack_mode = p }
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
        @upstream_set_configs.delete(name)
        @upstream_sets.delete(name).try &.each { |u| u.delete if set_copy?(u) }
        @log.info { "Upstream set '#{name}' deleted" }
      end

      def get_set(name)
        get_set?(name) || raise KeyError.new("Upstream set '#{name}' not found")
      end

      def get_set?(name)
        case name
        when "all"
          @upstreams.values
        else
          @upstream_sets[name]?
        end
      end

      def stop_all
        @upstreams.each_value &.close
        @upstream_sets.values.flatten.each &.close
      end
    end
  end
end

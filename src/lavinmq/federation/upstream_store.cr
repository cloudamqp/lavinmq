require "./upstream"

module LavinMQ
  module Federation
    class UpstreamStore
      include Enumerable(Upstream)
      Log = ::Log.for("federation.upstream_store")
      @upstreams = Hash(String, Upstream).new
      @upstream_sets = Hash(String, Array(Upstream)).new

      def initialize(@vhost : VHost)
        @metadata = ::Log::Metadata.new(nil, {vhost: @vhost.name})
      end

      def each(&)
        @upstreams.each_value do |v|
          yield v
        end
      end

      def create_upstream(name, config)
        do_delete_upstream(name)
        uri = config["uri"].to_s
        prefetch = config["prefetch-count"]?.try(&.as_i.to_u16) || Upstream::DEFAULT_PREFETCH
        reconnect_delay = config["reconnect-delay"]?.try(&.as_i?) || Upstream::DEFAULT_RECONNECT_DELAY
        ack_mode_str = config["ack-mode"]?.try(&.as_s.delete("-")).to_s
        ack_mode = AckMode.parse?(ack_mode_str) || Upstream::DEFAULT_ACK_MODE
        exchange = config["exchange"]?.try(&.as_s)
        max_hops = config["max-hops"]?.try(&.as_i64?) || Upstream::DEFAULT_MAX_HOPS
        expires = config["expires"]?.try(&.as_i64?) || Upstream::DEFAULT_EXPIRES
        msg_ttl = config["message-ttl"]?.try(&.as_i64?) || Upstream::DEFAULT_MSG_TTL
        consumer_tag = config["consumer-tag"]?.try(&.as_s?) || "federation-link-#{name}"
        # trust_user_id
        queue = config["queue"]?.try(&.as_s)
        @upstreams[name] = Upstream.new(@vhost, name, uri, exchange, queue, ack_mode, expires,
          max_hops, msg_ttl, prefetch, reconnect_delay, consumer_tag)
        Log.info &.emit "Upstream '#{name}' created", @metadata
        @upstreams[name]
      end

      def add(upstream : Upstream)
        @upstreams[upstream.name]?.try &.close
        @upstreams[upstream.name] = upstream
      end

      def delete_upstream(name)
        do_delete_upstream(name)
        Log.info &.emit "Upstream '#{name}' deleted", @metadata
      end

      private def do_delete_upstream(name)
        @upstreams.delete(name).try(&.close)
        @upstream_sets.each do |_, set|
          set.reject! do |upstream|
            return false unless upstream.name == name
            upstream.close
            true
          end
        end
      end

      def link(name, resource : Queue | Exchange)
        @upstreams[name]?.try &.link(resource)
      end

      def stop_link(resource : Queue | Exchange)
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
            config["uri"]?.try { |p| upstream.uri = URI.parse(p.as_a.first.to_s) }
            config["prefetch-count"]?.try { |p| upstream.prefetch = p.as_i.to_u16 }
            config["reconnect-delay"]?.try { |p| upstream.reconnect_delay = p.as_i }
            ack_mode_str = config["ack-mode"]?.try(&.as_s.delete("-")).to_s
            AckMode.parse?(ack_mode_str).try { |p| upstream.ack_mode = p }
            config["exchange"]?.try { |p| upstream.exchange = p.as_s }
            config["max-hops"]?.try { |p| upstream.max_hops = p.as_i64 }
            config["expires"]?.try { |p| upstream.expires = p.as_i64 }
            config["message-ttl"]?.try { |p| upstream.msg_ttl = p.as_i64 }
            config["queue"]?.try { |p| upstream.queue = p.as_s }
          end
          upstreams << upstream
        end
        @upstream_sets[name] = upstreams
      end

      def delete_upstream_set(name)
        @upstream_sets.delete(name)
        Log.info &.emit "Upstream set '#{name}' deleted", @metadata
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

require "./upstream"

module AvalancheMQ
  module Federation
    class UpstreamStore
      include Enumerable(Upstream)
      @upstreams = Hash(String, Upstream).new
      @upstream_sets = Hash(String, Array(Upstream)).new

      def initialize(@vhost : VHost)
      end

      def each
        @upstreams.values.each { |e| yield e }
      end

      def empty?
        @upstreams.empty?
      end

      def create_upstream(name, config)
        delete_upstream(name)
        uri = config["uri"].to_s
        prefetch = config["prefetch-count"]?.try(&.as_i) || Upstream::DEFAULT_PREFETCH
        reconnect_delay = config["reconnect-delay"]?.try(&.as_i) || Upstream::DEFUALT_RECONNECT_DELAY
        ack_mode_str = config["ack-mode"]?.try(&.as_s.delete("-")).to_s
        ack_mode = AckMode.parse?(ack_mode_str) || Upstream::DEFAULT_ACK_MODE
        if config["exchange"]?
          exchange = config["exchange"].as_s
          max_hops = config["max-hops"]?.try(&.as_i) || Upstream::DEFAULT_MAX_HOPS
          expires = config["expires"]?.try(&.as_s) || Upstream::DEFAULT_EXPIRES
          msg_ttl = config["message-ttl"]?.try(&.as_s) || Upstream::DEFAULT_MSG_TTL
          @upstreams[name] = ExchangeUpstream.new(@vhost, name, uri, exchange, max_hops, expires,
            msg_ttl, prefetch, reconnect_delay, ack_mode)
        else
          queue = config["queue"]?.try(&.as_s)
          @upstreams[name] = QueueUpstream.new(@vhost, name, uri, queue, prefetch, reconnect_delay,
            ack_mode)
        end
        @upstreams[name]
      end

      def delete_upstream(name)
        @upstreams.delete(name).try(&.close)
        @upstream_sets.each do |_, set|
          set.reject! do |upstream|
            return false unless upstream.name == name
            upstream.close
            true
          end
        end
        @vhost.log.info { "Upstream '#{name}' deleted" }
      end

      def link(name, resource : Queue)
        @upstreams[name]?.try &.as(QueueUpstream).link(resource)
      end

      def link(name, resource : Exchange)
        @upstreams[name]?.try &.as(ExchangeUpstream).link(resource)
      end

      def stop_link(resource : Queue)
        each do |upstream|
          next if upstream.is_a?(ExchangeUpstream)
          upstream.as(QueueUpstream).stop_link(resource)
        end
      end

      def stop_link(resource : Exchange)
        each do |upstream|
          next if upstream.is_a?(QueueUpstream)
          upstream.as(ExchangeUpstream).stop_link(resource)
        end
      end

      def create_upstream_set(name, config)
        delete_upstream_set(name)
        upstreams = Array(Upstream).new
        config.as_a.each do |cfg|
          upstream = @upstreams[cfg["upstream"].as_s]
          if (cfg.as_h.keys.size > 1)
            upstream = upstream.dup
            config["uri"]?.try { |p| upstream.uri = URI.parse(p.as_a.first.to_s) }
            config["prefetch-count"]?.try { |p| upstream.prefetch = p.as_i.to_u16 }
            config["reconnect-delay"]?.try { |p| upstream.reconnect_delay = p.as_i }
            ack_mode_str = config["ack-mode"]?.try(&.as_s.delete("-")).to_s
            AckMode.parse?(ack_mode_str).try { |p| upstream.ack_mode = p }
            if upstream.is_a?(ExchangeUpstream)
              config["exchange"]?.try { |p| upstream.exchange = p.as_s }
              config["max-hops"]?.try { |p| upstream.max_hops = p.as_i }
              config["expires"]?.try { |p| upstream.expires = p.as_s }
              config["message-ttl"]?.try { |p| upstream.msg_ttl = p.as_s }
            elsif upstream.is_a?(QueueUpstream)
              config["queue"]?.try { |p| upstream.queue = p.as_s }
            end
          end
          upstreams << upstream
        end
        @upstream_sets[name] = upstreams
      end

      def delete_upstream_set(name)
        @upstream_sets.delete(name)
        @vhost.log.info { "Upstream set '#{name}' deleted" }
      end

      def link_set(name, resource : Queue)
        set = get_set(name)
        set.each do |upstream|
          upstream.as(QueueUpstream).link(resource)
        end
      end

      def link_set(name, resource : Exchange)
        set = get_set(name)
        set.each do |upstream|
          upstream.as(ExchangeUpstream).link(resource)
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
        @upstreams.each { |_, upstream| upstream.close }
        @upstream_sets.values.flatten.each { |upstream| upstream.close }
      end
    end
  end
end

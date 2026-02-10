require "sync/exclusive"
require "./upstream"
require "../logger"

module LavinMQ
  module Federation
    class UpstreamStore
      include Enumerable(Upstream)
      Log = LavinMQ::Log.for "federation.upstream_store"
      @lock : Sync::Exclusive(NamedTuple(upstreams: Hash(String, Upstream), sets: Hash(String, Array(Upstream))))

      def initialize(@vhost : VHost)
        @metadata = ::Log::Metadata.new(nil, {vhost: @vhost.name})
        @log = Logger.new(Log, @metadata)
        @lock = Sync::Exclusive.new({upstreams: Hash(String, Upstream).new, sets: Hash(String, Array(Upstream)).new})
      end

      def each(&)
        upstreams = @lock.lock(&.[:upstreams].values)
        upstreams.each do |v|
          yield v
        end
      end

      def []?(name : String) : Upstream?
        @lock.lock { |l| l[:upstreams][name]? }
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
        new_upstream = Upstream.new(@vhost, name, uri, exchange, queue, ack_mode, expires,
          max_hops, msg_ttl, prefetch, reconnect_delay, consumer_tag)
        prev = @lock.lock do |l|
          prev_upstream = l[:upstreams].delete(name)
          l[:sets].each do |_, set|
            set.reject! { |u| u.name == name }
          end
          l[:upstreams][name] = new_upstream
          prev_upstream
        end
        prev.try(&.close)
        @log.info { "Upstream '#{name}' created" }
        new_upstream
      end

      def add(upstream : Upstream)
        prev = @lock.lock do |l|
          prev_upstream = l[:upstreams][upstream.name]?
          l[:upstreams][upstream.name] = upstream
          prev_upstream
        end
        prev.try &.close
      end

      def delete_upstream(name)
        prev = @lock.lock do |l|
          prev_upstream = l[:upstreams].delete(name)
          l[:sets].each do |_, set|
            set.reject! { |u| u.name == name }
          end
          prev_upstream
        end
        prev.try(&.close)
        @log.info { "Upstream '#{name}' deleted" }
      end

      def link(name, resource : Queue | Exchange)
        upstream = @lock.lock { |l| l[:upstreams][name]? }
        upstream.try &.link(resource)
      end

      def stop_link(resource : Queue | Exchange)
        upstreams = @lock.lock(&.[:upstreams].values)
        upstreams.each do |upstream|
          upstream.stop_link(resource)
        end
      end

      def create_upstream_set(name, config)
        @lock.lock do |l|
          l[:sets].delete(name)
          set = Array(Upstream).new
          config.as_a.each do |cfg|
            upstream = l[:upstreams][cfg["upstream"].as_s]
            if cfg.as_h.keys.size > 1
              upstream = upstream.dup
              config["uri"]?.try { |p| upstream.uri = URI.parse(p.as_a.first.to_s) }
              config["prefetch-count"]?.try { |p| upstream.prefetch = p.as_i.to_u16 }
              config["reconnect-delay"]?.try { |p| upstream.reconnect_delay = p.as_i.seconds }
              ack_mode_str = config["ack-mode"]?.try(&.as_s.delete("-")).to_s
              AckMode.parse?(ack_mode_str).try { |p| upstream.ack_mode = p }
              config["exchange"]?.try { |p| upstream.exchange = p.as_s }
              config["max-hops"]?.try { |p| upstream.max_hops = p.as_i64 }
              config["expires"]?.try { |p| upstream.expires = p.as_i64 }
              config["message-ttl"]?.try { |p| upstream.msg_ttl = p.as_i64 }
              config["queue"]?.try { |p| upstream.queue = p.as_s }
            end
            set << upstream
          end
          l[:sets][name] = set
        end
      end

      def delete_upstream_set(name)
        @lock.lock(&.[:sets].delete(name))
        @log.info { "Upstream set '#{name}' deleted" }
      end

      def link_set(name, resource : Exchange | Queue)
        set = get_set(name)
        set.each do |upstream|
          upstream.link(resource)
        end
      end

      def get_set(name)
        @lock.lock do |l|
          case name
          when "all"
            l[:upstreams].values
          else
            l[:sets][name].dup
          end
        end
      end

      def stop_all
        all = @lock.lock do |l|
          l[:upstreams].values + l[:sets].values.flatten
        end
        all.each &.close
      end
    end
  end
end

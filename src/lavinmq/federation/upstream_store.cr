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
      # Serializes upstream and set changes with policy reconciles, held
      # across the resource deletes' wait on the upstream broker
      @change_lock = Mutex.new

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
        @change_lock.synchronize { do_create_upstream(name, config) }
      end

      private def do_create_upstream(name, config)
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
      # sets that name it so a policy re-apply can link through them again.
      # The rebuild deletes and re-inserts each config, so iterate a snapshot
      private def rebuild_sets_with(name)
        rebuild = @upstream_set_configs.select do |_, config|
          config.as_a.any? { |cfg| cfg["upstream"].as_s == name }
        end
        rebuild.each { |set_name, config| do_create_upstream_set(set_name, config) }
      end

      def add(upstream : Upstream)
        @upstreams[upstream.name]?.try &.close
        @upstreams[upstream.name] = upstream
      end

      def delete_upstream(name)
        @change_lock.synchronize { do_delete_upstream(name) }
        @log.info { "Upstream '#{name}' deleted" }
      end

      private def do_delete_upstream(name)
        remove_upstream(name, &.delete)
      end

      # Remove the upstream and its entries from every set. The block decides
      # what happens to the resources its links created on the upstream
      # broker: delete removes them, close keeps them for a replacement's
      # links to reuse
      private def remove_upstream(name, &)
        @upstreams.delete(name).try { |u| yield u }
        @upstream_sets.each do |_, set|
          set.reject! do |upstream|
            next false unless upstream.name == name
            yield upstream
            true
          end
        end
      end

      # Whether the resources on the upstream broker can be reused: the
      # settings that name them, plus the declare arguments that a passive
      # re-declare would silently ignore
      private def same_resources?(old : Upstream, uri, exchange, queue, expires, msg_ttl, max_hops) : Bool
        same_broker?(old.uri, URI.parse(uri)) && old.exchange == exchange && old.queue == queue &&
          old.expires == expires && old.msg_ttl == msg_ttl && old.max_hops == max_hops
      end

      # Credentials and query options don't change which resources the links
      # declare, only the broker and its vhost (the path) do, compared as the
      # client defaults them
      private def same_broker?(a : URI, b : URI) : Bool
        a.scheme == b.scheme && a.host == b.host &&
          effective_port(a) == effective_port(b) &&
          (a.path.presence || "/") == (b.path.presence || "/")
      end

      private def effective_port(uri : URI) : Int32
        uri.port || (uri.scheme == "amqps" ? 5671 : 5672)
      end

      private def close_upstream(name)
        remove_upstream(name, &.close)
      end

      # Make the resource's links match its policy after a policy (re)apply.
      # A link whose upstream the policy still names is kept untouched: that
      # keeps the upstream queue, and its buffered messages, alive. The other
      # links are stopped and their upstream resources removed.
      def reconcile_link(resource : AMQP::Queue | Exchange, upstream_name : String?, set_name : String?)
        @change_lock.synchronize { do_reconcile_link(resource, upstream_name, set_name) }
      end

      private def do_reconcile_link(resource : AMQP::Queue | Exchange, upstream_name : String?, set_name : String?)
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
        @change_lock.synchronize { do_create_upstream_set(name, config) }
      end

      private def do_create_upstream_set(name, config)
        # Built first: a malformed entry must leave the old set running
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
        old_set = @upstream_sets.delete(name)
        # Retired before the new set is stored, so a delete can't race a new
        # link's redeclare of the same queue
        old_set.try { |set| retire_copies(set, upstreams) }
        # Stored only once the whole config built, so a rejected config can't
        # break the set rebuilds of later upstream parameter PUTs
        @upstream_set_configs[name] = config
        @upstream_sets[name] = upstreams
      end

      # Stops every copy in the old set and deletes an exchange link's
      # upstream resources unless an entry in the new set reuses them
      private def retire_copies(old_set : Array(Upstream), upstreams : Array(Upstream))
        old_set.each do |old|
          next unless set_copy?(old)
          old.links.each do |link|
            next unless link.is_a?(Upstream::ExchangeLink)
            link.delete unless resources_reused?(link, old, upstreams)
          end
          old.close
        end
      end

      # Same queue name on the same broker, with the declare arguments a
      # passive re-declare would silently ignore
      private def resources_reused?(link : Upstream::ExchangeLink, old : Upstream, upstreams : Array(Upstream)) : Bool
        upstreams.any? do |u|
          same_broker?(u.uri, old.uri) &&
            u.upstream_queue_name(link.federated_ex) == link.upstream_q &&
            u.expires == old.expires && u.msg_ttl == old.msg_ttl && u.max_hops == old.max_hops
        end
      end

      def delete_upstream_set(name)
        @change_lock.synchronize do
          @upstream_set_configs.delete(name)
          @upstream_sets.delete(name).try { |set| retire_copies(set, Array(Upstream).new) }
        end
        @log.info { "Upstream set '#{name}' deleted" }
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

require "./upstream"
require "../logger"

module LavinMQ
  module Federation
    class UpstreamStore
      include Enumerable(Upstream)
      Log = LavinMQ::Log.for "federation.upstream_store"
      # How long #stop_all waits for links to stop and upstream cleanups to
      # finish, e.g. for a link still connecting to an unreachable upstream
      STOP_TIMEOUT = 5.seconds

      # A retired exchange link, the broker and queue its delete removes, and
      # the channel closed once the delete is done
      alias PendingDelete = {Upstream::ExchangeLink, {String, String}, Channel(Nil)}

      # The links a change took out of the store. Stopping them waits for the
      # upstream and deleting connects to it, so both happen once the lock is
      # released
      private class Retired
        getter stops = Array(Upstream::Link).new
        getter deletes = Array(PendingDelete).new
      end

      @upstreams = Hash(String, Upstream).new
      @upstream_sets = Hash(String, Array(Upstream)).new
      @upstream_set_configs = Hash(String, JSON::Any).new
      # Serializes upstream and set changes with policy reconciles. Declares
      # take it through reconcile_link while holding the vhost's definitions
      # lock, so it guards in-memory state only: never connect to or wait on
      # a broker while holding it
      @change_lock = Mutex.new
      # Upstream resource deletes in flight, by broker and queue name
      @pending_deletes = Hash({String, String}, Array(Channel(Nil))).new
      # Link stops and deletes running in their own fibers, each closes its
      # channel when done
      @in_flight = Array(Channel(Nil)).new

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
        change { |retired| do_create_upstream(name, config, retired) }
      end

      private def do_create_upstream(name, config, retired)
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
          close_upstream(name, retired)
        else
          do_delete_upstream(name, retired)
        end
        @upstreams[name] = Upstream.new(@vhost, name, uri, exchange, queue, ack_mode, expires,
          max_hops, msg_ttl, prefetch, reconnect_delay, consumer_tag)
        rebuild_sets_with(name, retired)
        @log.info { "Upstream '#{name}' created" }
        @upstreams[name]
      end

      # Replacing an upstream drops its entries from every set; rebuild the
      # sets that name it so a policy re-apply can link through them again.
      # The rebuild deletes and re-inserts each config, so iterate a snapshot
      private def rebuild_sets_with(name, retired)
        rebuild = @upstream_set_configs.select do |_, config|
          config.as_a.any? { |cfg| cfg["upstream"].as_s == name }
        end
        rebuild.each { |set_name, config| do_create_upstream_set(set_name, config, retired) }
      end

      def add(upstream : Upstream)
        @upstreams[upstream.name]?.try &.close
        @upstreams[upstream.name] = upstream
      end

      def delete_upstream(name)
        change { |retired| do_delete_upstream(name, retired) }
        @log.info { "Upstream '#{name}' deleted" }
      end

      private def do_delete_upstream(name, retired)
        remove_upstream(name) do |upstream|
          upstream.take_links.each { |link| retire(link, upstream.uri, retired) }
        end
      end

      # Remove the upstream and its entries from every set. The block decides
      # what happens to the resources its links created on the upstream
      # broker: retiring deletes them, a plain stop keeps them for a
      # replacement's links to reuse
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

      private def same_broker?(a : URI, b : URI) : Bool
        broker_id(a) == broker_id(b)
      end

      # The broker and vhost a uri points to, as the client defaults them.
      # Credentials and query options don't change which resources the links
      # declare
      private def broker_id(uri : URI) : String
        port = uri.port || (uri.scheme == "amqps" ? 5671 : 5672)
        "#{uri.scheme}://#{uri.host}:#{port}#{uri.path.presence || "/"}"
      end

      private def close_upstream(name, retired)
        remove_upstream(name) { |upstream| retired.stops.concat upstream.take_links }
      end

      # Make the resource's links match its policy after a policy (re)apply.
      # A link whose upstream the policy still names is kept untouched: that
      # keeps the upstream queue, and its buffered messages, alive. The other
      # links are stopped and their upstream resources removed.
      def reconcile_link(resource : AMQP::Queue | Exchange, upstream_name : String?, set_name : String?)
        change { |retired| do_reconcile_link(resource, upstream_name, set_name, retired) }
      end

      private def do_reconcile_link(resource : AMQP::Queue | Exchange, upstream_name : String?, set_name : String?, retired)
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
          next if desired.any? &.same?(linked)
          linked.unlink(resource).try { |link| retire(link, linked.uri, retired) }
        end
        desired.each &.link(resource)
      end

      def stop_link(resource : AMQP::Queue | Exchange)
        change do |retired|
          each_linked_upstream do |upstream|
            upstream.unlink(resource).try { |link| retire(link, upstream.uri, retired) }
          end
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
        change { |retired| do_create_upstream_set(name, config, retired) }
      end

      private def do_create_upstream_set(name, config, retired)
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
        old_set = @upstream_sets[name]?
        # Stored only once the whole config built, so a rejected config can't
        # break the set rebuilds of later upstream parameter PUTs
        @upstream_set_configs[name] = config
        @upstream_sets[name] = upstreams
        old_set.try { |set| retire_copies(set, upstreams, retired) }
      end

      # Stops every copy in the old set and deletes an exchange link's
      # upstream resources unless an entry in the new set reuses them
      private def retire_copies(old_set : Array(Upstream), upstreams : Array(Upstream), retired)
        old_set.each do |old|
          next unless set_copy?(old)
          old.take_links.each do |link|
            if link.is_a?(Upstream::ExchangeLink) && resources_reused?(link, old, upstreams)
              retired.stops << link
            else
              retire(link, old.uri, retired)
            end
          end
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
        change do |retired|
          @upstream_set_configs.delete(name)
          @upstream_sets.delete(name).try { |set| retire_copies(set, Array(Upstream).new, retired) }
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

      # Waits for deletes in flight of this queue on this broker, so a link
      # never declares resources that a retired link is about to remove
      def await_pending_deletes(uri : URI, queue : String) : Nil
        key = {broker_id(uri), queue}
        pending = @change_lock.synchronize { @pending_deletes[key]?.try &.dup }
        pending.try &.each(&.receive?)
      end

      # Applies a change under the lock, then stops the links it took out of
      # the store and deletes their upstream resources in their own fibers
      private def change(&)
        retired = Retired.new
        @change_lock.synchronize { yield retired }
      ensure
        retired.try { |r| finish(r) }
      end

      # Queues a link the store no longer owns for a stop. An exchange link's
      # upstream resources are deleted too, and links declaring the same queue
      # wait for that (see #await_pending_deletes)
      private def retire(link : Upstream::Link, uri : URI, retired : Retired)
        unless link.is_a?(Upstream::ExchangeLink)
          retired.stops << link
          return
        end
        key = {broker_id(uri), link.upstream_q}
        done = Channel(Nil).new
        (@pending_deletes[key] ||= Array(Channel(Nil)).new) << done
        retired.deletes << {link, key, done}
      end

      private def finish(retired : Retired)
        retired.stops.each do |link|
          in_flight("Federation link stop vhost=#{@vhost.name}") do
            link.stop
            link.wait_stopped
          end
        end
        retired.deletes.each do |link, key, done|
          in_flight("Federation cleanup vhost=#{@vhost.name} queue=#{key[1]}") do
            begin
              link.delete
            ensure
              @change_lock.synchronize do
                if pending = @pending_deletes[key]?
                  pending.delete(done)
                  @pending_deletes.delete(key) if pending.empty?
                end
              end
              done.close
            end
            link.wait_stopped
          end
        end
      end

      # Runs a link's stop or delete in its own fiber, which #stop_all waits for
      private def in_flight(name : String, &block : ->) : Nil
        done = Channel(Nil).new
        @change_lock.synchronize { @in_flight << done }
        spawn(name: name) do
          block.call
        ensure
          @change_lock.synchronize { @in_flight.delete(done) }
          done.close
        end
      end

      # Stops all links, and waits up to *timeout* for them and for the stops
      # and deletes in flight, so that no upstream connection outlives a
      # broker shutdown. A link still connecting to an unreachable upstream
      # is left behind then.
      def stop_all(timeout : Time::Span = STOP_TIMEOUT) : Nil
        deadline = Time.instant + timeout
        links = Array(Upstream::Link).new
        @change_lock.synchronize do
          each_linked_upstream { |upstream| links.concat upstream.take_links }
        end
        links.each do |link|
          in_flight("Federation link stop vhost=#{@vhost.name}") do
            link.stop
            link.wait_stopped
          end
        end
        pending = @change_lock.synchronize { @in_flight.dup }
        pending.each do |done|
          select
          when done.receive?
          when timeout({deadline - Time.instant, Time::Span.zero}.max)
            left = pending.reject(&.closed?)
            @log.warn { "Gave up waiting for #{left.size} federation link stops and upstream cleanups" }
            # So that a later call, e.g. from VHost#close, doesn't wait for them again
            @change_lock.synchronize { left.each { |d| @in_flight.delete(d) } }
            break
          end
        end
      end
    end
  end
end

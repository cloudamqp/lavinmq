require "./amqp"
require "./rough_time"
require "../stdlib/*"
require "./persister"
require "./vhost_store"
require "./auth/user_store"
require "./exchange"
require "./amqp/queue"
require "./parameter"
require "./config"
require "./client/client"
require "./stats"
require "./in_memory_backend"
require "./auth/chain"
require "./auth/jwt/jwks_fetcher"

module LavinMQ
  class Server
    PROCESS_START = Time.instant

    getter vhosts, users, data_dir, parameters, authenticator
    include ParameterTarget

    @closed = BoolChannel.new(false)
    @flow = Atomic(Bool).new(true)

    def closed? : Bool
      @closed.value
    end

    def flow? : Bool
      @flow.get(:acquire)
    end

    @replicator : Clustering::Replicator?
    @log_exchange_channel : Channel(::Log::Entry)?
    Log = LavinMQ::Log.for "server"

    def initialize(@config : Config, @replicator = nil, authenticator : Auth::Authenticator? = nil)
      # Seed from rusage so counters survive Server re-creation on leader transitions.
      rusage = System.resource_usage
      @user_time = rusage.user_time.total_milliseconds.to_i64
      @sys_time = rusage.sys_time.total_milliseconds.to_i64
      @blocks_in = rusage.blocks_in.to_i64
      @blocks_out = rusage.blocks_out.to_i64

      @data_dir = @config.data_dir
      Dir.mkdir_p @data_dir
      Schema.migrate(@data_dir, @replicator)
      @persister = Persister.new(@data_dir, @replicator)
      @users = Auth::UserStore.new(@data_dir, @replicator)
      @vhosts = VHostStore.new(@data_dir, @users, @replicator, @persister)
      @parameters = ParameterStore(Parameter).new(@data_dir, "parameters.json", @replicator)
      @authenticator = authenticator || Auth::Chain.create(@config, @users)
      if @config.tcp_proxy_protocol? && @config.proxy_protocol_trusted_sources.empty?
        Log.warn { "PROXY protocol enabled without trusted sources configured - accepting from all sources" }
      end
      apply_parameter
      spawn stats_loop, name: "Server#stats_loop"
    end

    getter persister : Persister

    def followers
      @replicator.try(&.followers) || Array(Clustering::Follower).new
    end

    def syncing_followers
      @replicator.try(&.syncing_followers) || Array(Clustering::Follower).new
    end

    def all_followers
      @replicator.try(&.all_followers) || Array(Clustering::Follower).new
    end

    def stop
      return if @closed.swap(true)
      close_log_exchange
      @persister.close
      @vhosts.close
      @replicator.try &.clear
      @authenticator.try &.cleanup
      Fiber.yield
    end

    def connections : Array(Client)
      @vhosts.values.flat_map(&.connections)
    end

    def start_log_exchange
      return unless @config.log_exchange?
      return if @log_exchange_channel

      exchange_name = "amq.lavinmq.log"
      unless vhost = @vhosts["/"]?
        Log.warn { "log_exchange enabled but default vhost \"/\" is missing, skipping" }
        return
      end
      vhost.declare_exchange(exchange_name, "topic", true, false, true)
      @log_exchange_channel = log_channel = ::Log::InMemoryBackend.instance.add_channel
      spawn(name: "Log Exchange") do
        while entry = log_channel.receive?
          vhost.publish(msg: Message.new(
            exchange_name,
            entry.severity.to_s,
            "#{entry.source} - #{entry.message}",
            AMQP::Properties.new(timestamp: entry.timestamp, content_type: "text/plain")
          ))
        end
      end
    end

    def listen_clustering(server : TCPServer)
      @replicator.try &.listen(server)
    end

    def close
      return if @closed.swap(true)
      close_log_exchange
      @persister.close
      Log.debug { "Closing vhosts" }
      @vhosts.close
    end

    private def close_log_exchange
      if log_channel = @log_exchange_channel
        @log_exchange_channel = nil
        ::Log::InMemoryBackend.instance.remove_channel(log_channel)
        log_channel.close
      end
    end

    def add_parameter(parameter : Parameter, save = true)
      @parameters.create parameter, save: save
      apply_parameter(parameter)
    end

    # Persist the global parameter store; used to flush after a bulk import that
    # created parameters with save: false.
    def save_parameters!
      @parameters.save!
    end

    def delete_parameter(component_name, parameter_name)
      @parameters.delete({component_name, parameter_name})
    end

    private def apply_parameter(parameter : Parameter? = nil)
      @parameters.apply(parameter) do |p|
        Log.warn { "No action when applying parameter #{p.parameter_name}" }
      end
    end

    def update_stats_rates
      @vhosts.each_value do |vhost|
        vhost.each_queue(&.update_rates)
        vhost.each_exchange(&.update_rates)
        vhost.each_session(&.update_rates)
        vhost.each_connection do |connection|
          connection.update_rates
          connection.each_channel(&.update_rates)
        end
        vhost.update_rates
      end
    end

    def update_system_metrics(statm)
      interval = @config.stats_interval / 1000.0
      log_size = @config.stats_log_size
      rusage = System.resource_usage

      {% for m in METRICS %}
        until @{{ m.id }}_log.size < log_size
          @{{ m.id }}_log.shift
        end
        {% if m.id.ends_with? "_time" %}
          {{ m.id }} = rusage.{{ m.id }}.total_milliseconds.to_i64
          {{ m.id }}_rate = (({{ m.id }} - @{{ m.id }}) / (interval * 1000)).round(2)
        {% else %}
          {{ m.id }} = rusage.{{ m.id }}.to_i64
          {{ m.id }}_rate = (({{ m.id }} - @{{ m.id }}) / interval).round(2)
        {% end %}
        @{{ m.id }}_log.push {{ m.id }}_rate
        @{{ m.id }} = {{ m.id }}
      {% end %}

      until @rss_log.size < log_size
        @rss_log.shift
      end

      rss = statm_rss(statm) || ps_rss
      @rss = rss
      @rss_log.push @rss

      @mem_limit = cgroup_memory_max || System.physical_memory.to_i64

      begin
        fs_stats = Filesystem.info(@data_dir)
        until @disk_free_log.size < log_size
          @disk_free_log.shift
        end
        disk_free = fs_stats.available.to_i64
        @disk_free_log.push disk_free
        @disk_free = disk_free

        until @disk_total_log.size < log_size
          @disk_total_log.shift
        end
        disk_total = fs_stats.total.to_i64
        @disk_total_log.push disk_total
        @disk_total = disk_total
      rescue File::NotFoundError
        # Ignore when server is closed and deleted already
      end
    end

    private def stats_loop
      # statm holds rss value in linux
      if File.exists?("/proc/self/statm")
        statm = File.open("/proc/self/statm").tap &.read_buffering = false
      end
      until closed?
        @stats_collection_duration_seconds_total = Time.measure do
          @stats_rates_collection_duration_seconds = Time.measure do
            update_stats_rates
          end
          @stats_system_collection_duration_seconds = Time.measure do
            update_system_metrics(statm)
          end
        end
        @gc_stats = GC.prof_stats

        control_flow!
        sleep @config.stats_interval.milliseconds
      end
    ensure
      statm.try &.close
    end

    PAGE_SIZE = LibC.getpagesize

    # statm: https://man7.org/linux/man-pages/man5/proc.5.html
    # the second number in the output is the estimated RSS in pages
    private def statm_rss(statm) : Int64?
      return unless statm
      statm.rewind
      output = statm.gets_to_end
      if idx = output.index(' ', offset: 1)
        idx += 1
        if idx2 = output.index(' ', offset: idx)
          idx2 -= 1
          return output[idx..idx2].to_i64 * PAGE_SIZE
        end
      end
      Log.warn { "Could not parse /proc/self/statm: #{output}" }
    end

    # used on non linux systems
    private def ps_rss
      (`ps -o rss= -p $PPID`.to_i64? || 0i64) * 1024
    end

    # Available memory might be limited by a cgroup
    private def cgroup_memory_max : Int64?
      cgroup = File.read("/proc/self/cgroup")[/0::(.*)\n/, 1]? if File.exists?("/proc/self/cgroup")
      cgroup ||= "/"
      # cgroup v2
      begin
        return File.read("/sys/fs/cgroup#{cgroup}/memory.max").to_i64?
      rescue File::NotFoundError
      end
      # cgroup v1
      {
        "/sys/fs/cgroup#{cgroup}/memory.limit_in_bytes",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
      }.each do |path|
        l = File.read(path)
        return if l == "9223372036854771712\n" # Max in cgroup v1
        return l.to_i64?
      rescue File::NotFoundError
      end
    end

    METRICS = {:user_time, :sys_time, :blocks_out, :blocks_in}

    {% for m in METRICS %}
      getter {{ m.id }} : Int64
      getter {{ m.id }}_log = Deque(Float64).new(Config.instance.stats_log_size)
    {% end %}
    getter mem_limit = 0_i64
    getter rss = 0_i64
    getter rss_log = Deque(Int64).new(Config.instance.stats_log_size)
    getter disk_total = 0_i64
    getter disk_total_log = Deque(Int64).new(Config.instance.stats_log_size)
    getter disk_free = 0_i64
    getter disk_free_log = Deque(Int64).new(Config.instance.stats_log_size)
    getter stats_collection_duration_seconds_total = Time::Span.new
    getter stats_rates_collection_duration_seconds = Time::Span.new
    getter stats_system_collection_duration_seconds = Time::Span.new
    getter gc_stats = GC.prof_stats

    # Message stats for deleted vhosts, required to keep accurate global counters
    property deleted_vhosts_messages_delivered_total = 0_u64
    property deleted_vhosts_messages_redelivered_total = 0_u64
    property deleted_vhosts_messages_acknowledged_total = 0_u64
    property deleted_vhosts_messages_confirmed_total = 0_u64

    private def control_flow!
      if disk_full?
        if flow?
          Log.info { "Low disk space: #{@disk_free.humanize}B, stopping flow" }
          flow(false)
        end
      elsif !flow?
        Log.info { "Not low on disk space, starting flow" }
        flow(true)
      elsif disk_usage_over_warning_level?
        Log.info { "Low on disk space: #{@disk_free.humanize}B" }
      end
    end

    def disk_full?
      @disk_free < 3_i64 * @config.segment_size || @disk_free < @config.free_disk_min
    end

    def disk_usage_over_warning_level?
      @disk_free < 6_i64 * @config.segment_size || @disk_free < @config.free_disk_warn
    end

    def flow(active : Bool)
      @flow.set(active, :release)
      @vhosts.each_value &.flow=(active)
    end

    def uptime
      Time.instant - PROCESS_START
    end
  end
end

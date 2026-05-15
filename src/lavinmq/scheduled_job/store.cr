require "json"
require "amq-protocol"
require "./cron"
require "./runner"
require "../auth/user"
require "../bool_channel"

module LavinMQ
  module ScheduledJob
    class ConfigError < Exception; end

    class Store
      STATE_FILENAME = "scheduled_jobs_state.json"
      FLUSH_INTERVAL = 1.second
      Log            = LavinMQ::Log.for "scheduled_job_store"

      def initialize(@vhost : VHost)
        @jobs = Hash(String, Runner).new
        @stats = Hash(String, PersistedStats).new
        @state_path = File.join(@vhost.data_dir, STATE_FILENAME)
        @dirty = Atomic(Bool).new(false)
        @closed = Atomic(Bool).new(false)
        @flusher_stopped = BoolChannel.new(false)
        load_state!
        spawn run_flusher, name: "ScheduledJob state flusher vhost=#{@vhost.name}"
      end

      # Stops the flusher fiber, terminates every runner, and writes any
      # pending stats to disk. Safe to call more than once.
      def close : Nil
        return if @closed.swap(true)
        @jobs.each_value &.terminate
        @flusher_stopped.set(true)
        flush!
      end

      # Forces an immediate synchronous write of the stats file if any
      # runner has updated its stats since the last flush. No-op otherwise.
      def flush! : Nil
        return unless @dirty.swap(false)
        save_state!
      end

      def []?(name : String) : Runner?
        @jobs[name]?
      end

      def [](name : String) : Runner
        @jobs[name]
      end

      def each_value(& : Runner ->) : Nil
        @jobs.each_value { |j| yield j }
      end

      def values : Array(Runner)
        @jobs.values
      end

      def size : Int32
        @jobs.size
      end

      def has_key?(name : String) : Bool
        @jobs.has_key?(name)
      end

      def empty? : Bool
        @jobs.empty?
      end

      def self.validate_config!(config : JSON::Any, user : Auth::BaseUser?, vhost : VHost? = nil) : Nil
        unless config.as_h?
          raise ConfigError.new("Config must be a JSON object")
        end
        cron_str = require_string!(config, "cron", alias_key: "schedule", allow_empty: false)
        begin
          Cron.parse(cron_str)
        rescue ex : Cron::ParseError
          raise ConfigError.new("Invalid cron expression: #{ex.message}")
        end
        optional_string!(config, "exchange")
        require_string!(config, "routing-key", alias_key: "routing_key", allow_empty: false)
        require_string!(config, "body", allow_empty: true)
        optional_string!(config, "content-type")
        optional_string!(config, "content-encoding")
        if headers = config["headers"]?
          raise ConfigError.new("'headers' must be a JSON object") unless headers.as_h?
        end
        if user && vhost
          exchange = normalize_exchange(config["exchange"]?.try(&.as_s?))
          unless user.can_write?(vhost.name, exchange) && user.can_config?(vhost.name, exchange)
            raise ConfigError.new("#{user.name} can't publish to exchange '#{exchange}' in #{vhost.name}")
          end
        end
      end

      def create(name : String, config : JSON::Any) : Runner
        Store.validate_config!(config, user: nil)
        @jobs[name]?.try &.terminate
        cron_str = Store.require_string!(config, "cron", alias_key: "schedule", allow_empty: false)
        cron = Cron.parse(cron_str)
        exchange = Store.normalize_exchange(config["exchange"]?.try(&.as_s?))
        routing_key = Store.require_string!(config, "routing-key", alias_key: "routing_key", allow_empty: false)
        body = Store.require_string!(config, "body", allow_empty: true)
        properties = build_properties(config)
        runner = Runner.new(name, @vhost, cron, exchange, routing_key, body, properties,
          on_state_change: ->(r : Runner) { persist_runner_state(r) })
        if existing = @stats[name]?
          runner.restore_stats(existing.run_count, existing.last_run_at,
            existing.last_error, existing.last_duration_ms)
        end
        @jobs[name] = runner
        spawn runner.run, name: "ScheduledJob #{name} vhost=#{@vhost.name}"
        runner
      end

      # Returns the value of the required string field. Raises ConfigError with
      # a precise reason when the field is missing, the wrong JSON type, or
      # (when allow_empty: false) an empty string.
      protected def self.require_string!(config : JSON::Any, key : String,
                                         *, alias_key : String? = nil,
                                         allow_empty : Bool) : String
        present_key, raw = if v = config[key]?
                             {key, v}
                           elsif alias_key && (v = config[alias_key]?)
                             {alias_key, v}
                           else
                             missing = alias_key ? "'#{key}' (or '#{alias_key}')" : "'#{key}'"
                             raise ConfigError.new("Field #{missing} is required")
                           end
        s = raw.as_s? || raise ConfigError.new("Field '#{present_key}' must be a string")
        if !allow_empty && s.empty?
          raise ConfigError.new("Field '#{present_key}' must not be empty")
        end
        s
      end

      # Validates an optional string field's type. Returns the value or nil.
      protected def self.optional_string!(config : JSON::Any, key : String) : String?
        v = config[key]? || return nil
        v.as_s? || raise ConfigError.new("Field '#{key}' must be a string")
      end

      # The AMQP default exchange is the empty string; tools commonly write
      # "amq.default" to mean the same thing. Normalize both.
      protected def self.normalize_exchange(name : String?) : String
        return "" if name.nil?
        return "" if name == "amq.default"
        name
      end

      protected def self.lookup_string(config : JSON::Any, *keys : String) : String?
        keys.each do |k|
          if v = config[k]?
            if s = v.as_s?
              return s
            end
          end
        end
        nil
      end

      def delete(name : String) : Runner?
        if job = @jobs.delete name
          job.delete
          @stats.delete(name)
          @dirty.set(true)
          flush! # delete is rare and we want it durable immediately
          job
        end
      end

      private def build_properties(config : JSON::Any) : AMQ::Protocol::Properties
        headers = nil
        if h = config["headers"]?.try(&.as_h?)
          headers = AMQ::Protocol::Table.new
          h.each { |k, v| headers[k] = coerce_header_value(v) }
        end
        AMQ::Protocol::Properties.new(
          content_type: config["content-type"]?.try(&.as_s?),
          content_encoding: config["content-encoding"]?.try(&.as_s?),
          headers: headers,
          delivery_mode: 2_u8, # always persistent
        )
      end

      private def coerce_header_value(v : JSON::Any) : AMQ::Protocol::Field
        case raw = v.raw
        when String  then raw
        when Int64   then raw
        when Int32   then raw.to_i64
        when Float64 then raw
        when Bool    then raw
        when Nil     then ""
        else
          v.to_json
        end
      end

      private def persist_runner_state(runner : Runner) : Nil
        @stats[runner.name] = PersistedStats.new(
          run_count: runner.run_count,
          last_run_at: runner.last_run_at,
          last_error: runner.last_error,
          last_duration_ms: runner.last_duration_ms,
        )
        @dirty.set(true)
      end

      private def run_flusher : Nil
        loop do
          select
          when timeout FLUSH_INTERVAL
            flush!
          when @flusher_stopped.when_true.receive?
            return
          end
        end
      end

      private def save_state! : Nil
        tmp = "#{@state_path}.tmp"
        File.open(tmp, "w") do |f|
          JSON.build(f) do |json|
            json.object do
              json.field "jobs" do
                json.object do
                  @stats.each do |name, s|
                    json.field name do
                      s.to_json(json)
                    end
                  end
                end
              end
            end
          end
        end
        File.rename tmp, @state_path
        @vhost.replicator.try &.replace_file @state_path
      rescue ex
        Log.error(exception: ex) { "Failed to persist scheduled job state" }
      end

      private def load_state! : Nil
        return unless File.exists?(@state_path)
        File.open(@state_path) do |f|
          payload = StatePayload.from_json(f)
          @stats = payload.jobs
          @vhost.replicator.try &.register_file f
        end
      rescue ex
        Log.error(exception: ex) { "Failed to load scheduled job state; starting fresh" }
        @stats = Hash(String, PersistedStats).new
      end

      struct PersistedStats
        include JSON::Serializable

        property run_count : UInt64
        property last_run_at : Time?
        property last_error : String?
        property last_duration_ms : Int64?

        def initialize(@run_count : UInt64, @last_run_at : Time?,
                       @last_error : String?, @last_duration_ms : Int64?)
        end
      end

      struct StatePayload
        include JSON::Serializable

        property jobs : Hash(String, PersistedStats) = Hash(String, PersistedStats).new
      end
    end
  end
end

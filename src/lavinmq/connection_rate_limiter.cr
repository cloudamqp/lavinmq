module LavinMQ
  # Token bucket rate limiter for incoming connections.
  # Fiber-safe (single-threaded Crystal runtime).
  class ConnectionRateLimiter
    Log             = LavinMQ::Log.for "rate_limiter"
    MAX_TRACKED_IPS = 100_000

    private record PerIPState,
      tokens : Float64,
      last_refill : Time::Instant

    @global_tokens : Float64
    @global_last_refill : Time::Instant
    @last_log_time : Time::Instant
    @last_table_full_log_time : Time::Instant

    def initialize(@config : Config)
      @global_tokens = @config.connection_rate_limit.to_f
      @global_last_refill = Time.instant
      @per_ip = Hash(String, PerIPState).new
      @last_log_time = Time.instant
      @last_table_full_log_time = Time.instant
    end

    # Returns true if the connection should be allowed.
    # Per-IP is checked first to avoid consuming a global token
    # when the per-IP limit rejects the connection. Note: if per-IP
    # passes but global rejects, the per-IP token is still consumed.
    # This is acceptable since tokens refill continuously.
    def allow?(remote_address : String) : Bool
      return true unless rate_limiting_enabled?
      allow_per_ip?(remote_address) && allow_global?
    end

    private def rate_limiting_enabled? : Bool
      @config.connection_rate_limit > 0 ||
        @config.connection_rate_limit_per_ip > 0
    end

    private def allow_global? : Bool
      limit = @config.connection_rate_limit
      return true if limit <= 0

      now = Time.instant
      elapsed = (now - @global_last_refill).total_seconds
      @global_tokens = Math.min(
        limit.to_f,
        @global_tokens + elapsed * limit
      )
      @global_last_refill = now

      if @global_tokens >= 1.0
        @global_tokens -= 1.0
        true
      else
        false
      end
    end

    private def allow_per_ip?(ip : String) : Bool
      limit = @config.connection_rate_limit_per_ip
      return true if limit <= 0

      now = Time.instant
      unless @per_ip.has_key?(ip)
        if @per_ip.size >= MAX_TRACKED_IPS
          evict_oldest_entry
        end
        @per_ip[ip] = PerIPState.new(limit.to_f, now)
      end

      state = @per_ip[ip]
      elapsed = (now - state.last_refill).total_seconds
      tokens = Math.min(limit.to_f, state.tokens + elapsed * limit)

      if tokens >= 1.0
        @per_ip[ip] = PerIPState.new(tokens - 1.0, now)
        true
      else
        @per_ip[ip] = PerIPState.new(tokens, now)
        false
      end
    end

    def log_rate_limited(remote_address : String)
      now = Time.instant
      if (now - @last_log_time).total_seconds >= 1.0
        @last_log_time = now
        Log.warn { "Connection rate limited: #{remote_address}" }
      end
    end

    # Remove stale per-IP entries to prevent unbounded growth.
    # Called periodically (e.g. from stats_loop).
    def cleanup_stale_entries
      return if @config.connection_rate_limit_per_ip <= 0
      now = Time.instant
      @per_ip.reject! do |_ip, state|
        (now - state.last_refill).total_seconds > 60
      end
    end

    # Evict the oldest-inserted entry. Crystal's Hash is
    # insertion-ordered, so `first` is O(1).
    private def evict_oldest_entry
      now = Time.instant
      if (now - @last_table_full_log_time).total_seconds >= 1.0
        @last_table_full_log_time = now
        Log.warn { "Per-IP tracking limit reached (#{MAX_TRACKED_IPS}), evicting oldest entry" }
      end
      oldest_ip = @per_ip.first_key
      @per_ip.delete(oldest_ip)
    end
  end
end

module LavinMQ
  # Token bucket rate limiter for incoming connections.
  # Fiber-safe (single-threaded Crystal runtime).
  class ConnectionRateLimiter
    Log             = LavinMQ::Log.for "rate_limiter"
    MAX_TRACKED_IPS = 100_000

    @global_tokens : Float64
    @global_last_refill : Time::Instant
    @last_log_time : Time::Instant

    def initialize(@config : Config)
      @global_tokens = @config.connection_rate_limit.to_f
      @global_last_refill = Time.instant
      @per_ip_tokens = Hash(String, Float64).new
      @per_ip_last_refill = Hash(String, Time::Instant).new
      @last_log_time = Time.instant
      @log_suppressed = false
    end

    # Returns true if the connection should be allowed.
    # Per-IP is checked first to avoid consuming a global token
    # when the per-IP limit already rejects the connection.
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
      consume_token(
        limit,
        pointerof(@global_tokens),
        pointerof(@global_last_refill)
      )
    end

    private def allow_per_ip?(ip : String) : Bool
      limit = @config.connection_rate_limit_per_ip
      return true if limit <= 0

      now = Time.instant
      unless @per_ip_last_refill.has_key?(ip)
        if @per_ip_tokens.size >= MAX_TRACKED_IPS
          Log.warn { "Per-IP tracking limit reached (#{MAX_TRACKED_IPS}), rejecting new IP: #{ip}" }
          return false
        end
        @per_ip_tokens[ip] = limit.to_f
        @per_ip_last_refill[ip] = now
      end

      last_refill = @per_ip_last_refill[ip]
      tokens = @per_ip_tokens[ip]
      elapsed = (now - last_refill).total_seconds
      tokens = Math.min(limit.to_f, tokens + elapsed * limit)
      @per_ip_last_refill[ip] = now

      if tokens >= 1.0
        @per_ip_tokens[ip] = tokens - 1.0
        true
      else
        @per_ip_tokens[ip] = tokens
        false
      end
    end

    private def consume_token(
      limit : Int32,
      tokens_ptr : Pointer(Float64),
      last_refill_ptr : Pointer(Time::Instant),
    ) : Bool
      now = Time.instant
      elapsed = (now - last_refill_ptr.value).total_seconds
      tokens = Math.min(
        limit.to_f,
        tokens_ptr.value + elapsed * limit
      )
      last_refill_ptr.value = now

      if tokens >= 1.0
        tokens_ptr.value = tokens - 1.0
        true
      else
        tokens_ptr.value = tokens
        false
      end
    end

    def log_rate_limited(remote_address : String)
      now = Time.instant
      if (now - @last_log_time).total_seconds >= 1.0
        @last_log_time = now
        Log.warn { "Connection rate limited: #{remote_address}" }
        @log_suppressed = false
      elsif !@log_suppressed
        @log_suppressed = true
      end
    end

    # Remove stale per-IP entries to prevent unbounded growth.
    # Called periodically (e.g. from stats_loop).
    def cleanup_stale_entries
      return if @config.connection_rate_limit_per_ip <= 0
      now = Time.instant
      @per_ip_last_refill.reject! do |ip, last_refill|
        if (now - last_refill).total_seconds > 60
          @per_ip_tokens.delete(ip)
          true
        end
      end
    end
  end
end

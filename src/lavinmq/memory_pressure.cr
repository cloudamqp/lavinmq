require "log"

module LavinMQ
  # Advanced memory pressure monitoring inspired by Bun's memory awareness
  # Monitors memory usage and triggers GC when needed
  class MemoryPressureMonitor
    Log = LavinMQ::Log.for "memory_pressure"
    
    @mem_limit : Int64
    @high_watermark : Float64
    @critical_watermark : Float64
    @check_interval : Time::Span
    @last_gc : Time::Monotonic
    @gc_cooldown : Time::Span
    @enabled = Atomic(Bool).new(true)
    
    # Pressure levels
    enum Level
      Normal
      Moderate
      High
      Critical
    end
    
    def initialize(
      @mem_limit : Int64,
      @high_watermark : Float64 = 0.75,
      @critical_watermark : Float64 = 0.90,
      @check_interval : Time::Span = 5.seconds,
      @gc_cooldown : Time::Span = 10.seconds
    )
      @last_gc = Time.monotonic
      validate_parameters
    end
    
    private def validate_parameters
      raise ArgumentError.new("high_watermark must be between 0 and 1") unless 0 < @high_watermark < 1
      raise ArgumentError.new("critical_watermark must be between high_watermark and 1") unless @high_watermark < @critical_watermark < 1
    end
    
    # Start monitoring in a background fiber
    def start : Nil
      spawn(name: "MemoryPressureMonitor") do
        loop do
          break unless @enabled.get(:acquire)
          
          begin
            check_and_respond
          rescue ex
            Log.error(exception: ex) { "Error in memory pressure monitoring" }
          end
          
          sleep @check_interval
        end
      end
    end
    
    # Stop the monitor
    def stop : Nil
      @enabled.set(false, :release)
    end
    
    # Check memory pressure and respond accordingly
    private def check_and_respond : Nil
      level = current_pressure_level
      
      case level
      when Level::Critical
        handle_critical_pressure
      when Level::High
        handle_high_pressure
      when Level::Moderate
        handle_moderate_pressure
      else
        # Normal - no action needed
      end
    end
    
    # Calculate current memory pressure level
    def current_pressure_level : Level
      usage_ratio = memory_usage_ratio
      
      if usage_ratio >= @critical_watermark
        Level::Critical
      elsif usage_ratio >= @high_watermark
        Level::High
      elsif usage_ratio >= (@high_watermark * 0.6)
        Level::Moderate
      else
        Level::Normal
      end
    end
    
    # Get current memory usage ratio (0.0 to 1.0)
    private def memory_usage_ratio : Float64
      {% if flag?(:gc_none) %}
        # If GC is disabled, we can't get accurate stats
        return 0.0
      {% else %}
        stats = GC.prof_stats
        heap_size = stats.heap_size.to_f
        return heap_size / @mem_limit.to_f
      {% end %}
    end
    
    # Handle critical memory pressure
    private def handle_critical_pressure : Nil
      Log.warn { "Critical memory pressure detected, forcing aggressive GC" }
      force_gc
      
      # Log detailed stats after GC
      log_memory_stats("post-critical-gc")
    end
    
    # Handle high memory pressure
    private def handle_high_pressure : Nil
      # Only GC if we've passed the cooldown period
      if should_trigger_gc?
        Log.info { "High memory pressure, triggering GC" }
        force_gc
      end
    end
    
    # Handle moderate memory pressure
    private def handle_moderate_pressure : Nil
      # Just log, don't trigger GC yet
      if @last_gc + (@gc_cooldown * 3) < Time.monotonic
        Log.debug { "Moderate memory pressure (#{(memory_usage_ratio * 100).round(1)}%)" }
      end
    end
    
    # Check if we should trigger GC based on cooldown
    private def should_trigger_gc? : Bool
      Time.monotonic > @last_gc + @gc_cooldown
    end
    
    # Force garbage collection
    private def force_gc : Nil
      {% unless flag?(:gc_none) %}
        before = GC.prof_stats
        GC.collect
        @last_gc = Time.monotonic
        after = GC.prof_stats
        
        freed = before.heap_size - after.heap_size
        Log.debug { "GC freed #{freed.humanize_bytes}" }
      {% end %}
    end
    
    # Log detailed memory statistics
    private def log_memory_stats(context : String) : Nil
      {% unless flag?(:gc_none) %}
        stats = GC.prof_stats
        Log.info { "Memory stats (#{context}): heap=#{stats.heap_size.humanize_bytes}, " \
                  "free=#{stats.free_bytes.humanize_bytes}, " \
                  "unmapped=#{stats.unmapped_bytes.humanize_bytes}, " \
                  "usage=#{(memory_usage_ratio * 100).round(1)}%" }
      {% end %}
    end
    
    # Get current memory statistics
    def stats : NamedTuple
      {
        level: current_pressure_level,
        usage_ratio: memory_usage_ratio,
        mem_limit: @mem_limit,
        high_watermark: @high_watermark,
        critical_watermark: @critical_watermark,
        seconds_since_last_gc: (Time.monotonic - @last_gc).total_seconds,
      }
    end
  end
end

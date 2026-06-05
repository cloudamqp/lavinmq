require "./config"
require "./stat_log"

module LavinMQ
  module Stats
    macro rate_stats(stats_keys, log_keys = %w[])
      @_stats_log_capacity : Int32 = Config.instance.stats_log_size
      @_stats_log_head : Int32 = 0
      @_stats_log_size : Int32 = 0
      @_stats_const_run : Int32 = 0
      @_stats_rate_buffer : Pointer(Float64) = Pointer(Float64).null # null while constant
      {% if log_keys.size > 0 %}
        @_stats_count_buffer : Pointer(UInt32) = Pointer(UInt32).null
      {% end %}

      {% for name, i in stats_keys %}
        @{{ name.id }}_count = Atomic(UInt64).new(0_u64)
        @{{ name.id }}_count_prev = 0_u64
        @{{ name.id }}_rate = 0_f64
        def {{ name.id }}_count
          @{{ name.id }}_count.get(:relaxed)
        end

        def {{ name.id }}_log : StatLogView(Float64)
          if @_stats_rate_buffer.null?
            StatLogView(Float64).new(Pointer(Float64).null, 0, 0, @_stats_log_size, @_stats_log_capacity, @{{ name.id }}_rate)
          else
            StatLogView(Float64).new(@_stats_rate_buffer, {{ i }} * @_stats_log_capacity,
              @_stats_log_head, @_stats_log_size, @_stats_log_capacity, 0_f64)
          end
        end
      {% end %}
      {% for name, j in log_keys %}
        @{{ name.id }}_log_last = 0_u32
        def {{ name.id }}_log : StatLogView(UInt32)
          if @_stats_rate_buffer.null?
            StatLogView(UInt32).new(Pointer(UInt32).null, 0, 0, @_stats_log_size, @_stats_log_capacity, @{{ name.id }}_log_last)
          else
            StatLogView(UInt32).new(@_stats_count_buffer, {{ j }} * @_stats_log_capacity,
              @_stats_log_head, @_stats_log_size, @_stats_log_capacity, 0_u32)
          end
        end
      {% end %}

      def stats_details
        {
          {% for name in stats_keys %}
            {{ name.id }}: {{ name.id }}_count,
            {{ name.id }}_details: {
              rate: @{{ name.id }}_rate,
              log:  {{ name.id }}_log,
            },
          {% end %}
        }
      end

      # Like stats_details but without log
      def current_stats_details
        {
          {% for name in stats_keys %}
            {{ name.id }}: {{ name.id }}_count,
            {{ name.id }}_details: { rate: @{{ name.id }}_rate },
          {% end %}
        }
      end

      def update_rates : Nil
        # Float seconds so a sub-second stats_interval does not truncate to 0
        # and produce NaN/Inf rates.
        interval = Config.instance.stats_interval / 1000.0
        cap = @_stats_log_capacity

        {% for name, i in stats_keys %}
          new_count_{{ i }} = @{{ name.id }}_count.get(:relaxed)
          delta_{{ i }} = new_count_{{ i }} - @{{ name.id }}_count_prev
          new_rate_{{ i }} = delta_{{ i }}.zero? ? 0.0 : (delta_{{ i }} / interval).round(1)
        {% end %}
        {% for name, j in log_keys %}
          new_log_{{ j }} = {{ name.id }}
        {% end %}

        if @_stats_rate_buffer.null?
          diverged = false
          if @_stats_log_size > 0 # size 0: first sample just establishes the constant
            {% for name, i in stats_keys %}
              diverged = true if new_rate_{{ i }} != @{{ name.id }}_rate
            {% end %}
            {% for name, j in log_keys %}
              diverged = true if new_log_{{ j }} != @{{ name.id }}_log_last
            {% end %}
          end

          unless diverged
            {% for name, i in stats_keys %}
              @{{ name.id }}_rate = new_rate_{{ i }}
              @{{ name.id }}_count_prev = new_count_{{ i }}
            {% end %}
            {% for name, j in log_keys %}
              @{{ name.id }}_log_last = new_log_{{ j }}
            {% end %}
            @_stats_log_size += 1 if @_stats_log_size < cap
            return
          end

          filled = @_stats_log_size
          @_stats_rate_buffer = GC.malloc_atomic({{ stats_keys.size }} * cap * sizeof(Float64)).as(Pointer(Float64))
          {% if log_keys.size > 0 %}
            @_stats_count_buffer = GC.malloc_atomic({{ log_keys.size }} * cap * sizeof(UInt32)).as(Pointer(UInt32))
          {% end %}
          {% for name, i in stats_keys %}
            f = 0
            while f < filled
              @_stats_rate_buffer[{{ i }} * cap + f] = @{{ name.id }}_rate
              f += 1
            end
          {% end %}
          {% for name, j in log_keys %}
            f = 0
            while f < filled
              @_stats_count_buffer[{{ j }} * cap + f] = @{{ name.id }}_log_last
              f += 1
            end
          {% end %}
          @_stats_log_head = 0
        end

        tail = @_stats_log_head + @_stats_log_size
        tail -= cap if tail >= cap
        constant = true
        {% for name, i in stats_keys %}
          constant = false if new_rate_{{ i }} != @{{ name.id }}_rate
          @{{ name.id }}_rate = new_rate_{{ i }}
          @_stats_rate_buffer[{{ i }} * cap + tail] = new_rate_{{ i }}
          @{{ name.id }}_count_prev = new_count_{{ i }}
        {% end %}
        {% for name, j in log_keys %}
          constant = false if new_log_{{ j }} != @{{ name.id }}_log_last
          @_stats_count_buffer[{{ j }} * cap + tail] = new_log_{{ j }}
          @{{ name.id }}_log_last = new_log_{{ j }}
        {% end %}
        if @_stats_log_size < cap
          @_stats_log_size += 1
        else
          @_stats_log_head += 1
          @_stats_log_head -= cap if @_stats_log_head >= cap
        end

        if constant
          @_stats_const_run += 1
          if @_stats_const_run >= cap
            @_stats_rate_buffer = Pointer(Float64).null
            {% if log_keys.size > 0 %}
              @_stats_count_buffer = Pointer(UInt32).null
            {% end %}
            @_stats_log_head = 0
            @_stats_log_size = cap
            @_stats_const_run = 0
          end
        else
          @_stats_const_run = 1
        end
      end
    end
  end
end

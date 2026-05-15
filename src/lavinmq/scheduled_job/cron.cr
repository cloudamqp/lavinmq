module LavinMQ
  module ScheduledJob
    # Standard 5-field cron parser: "minute hour day-of-month month day-of-week".
    # Sunday is accepted as either 0 or 7. All times are interpreted as UTC.
    # No timezone, no @hourly aliases, no L/W modifiers.
    struct Cron
      class ParseError < Exception; end

      MAX_ITERATIONS = 4 * 366 * 24 * 60

      getter expression : String
      @minute : UInt64
      @hour : UInt64
      @dom : UInt64
      @month : UInt64
      @dow : UInt64
      @dom_restricted : Bool
      @dow_restricted : Bool

      def initialize(@expression, @minute, @hour, @dom, @month, @dow,
                     @dom_restricted, @dow_restricted)
      end

      def self.parse(expr : String) : Cron
        trimmed = expr.strip
        fields = trimmed.split(/\s+/)
        unless fields.size == 5
          raise ParseError.new("Expected 5 fields, got #{fields.size}: #{expr.inspect}")
        end
        minute = parse_field(fields[0], 0, 59)
        hour = parse_field(fields[1], 0, 23)
        dom = parse_field(fields[2], 1, 31)
        month = parse_field(fields[3], 1, 12)
        dow_raw = parse_field(fields[4], 0, 7)
        # Normalize Sunday: bit 7 -> bit 0
        dow = (dow_raw | (dow_raw >> 7)) & 0x7F_u64
        Cron.new(fields.join(' '), minute, hour, dom, month, dow,
          dom_restricted: fields[2] != "*",
          dow_restricted: fields[4] != "*")
      end

      # Returns the smallest Time > `time` that matches the expression.
      def next_after(time : Time) : Time
        # Round up to the next minute boundary, dropping seconds/nanoseconds.
        t = Time.utc(time.year, time.month, time.day, time.hour, time.minute) + 1.minute
        MAX_ITERATIONS.times do
          if @month.bit(t.month) == 0
            t = beginning_of_next_month(t)
            next
          end
          unless day_matches?(t)
            t = (t + 1.day).at_beginning_of_day
            next
          end
          if @hour.bit(t.hour) == 0
            t = Time.utc(t.year, t.month, t.day, t.hour, 0) + 1.hour
            next
          end
          if @minute.bit(t.minute) == 0
            t += 1.minute
            next
          end
          return t
        end
        raise ParseError.new("No future match for cron expression #{@expression.inspect}")
      end

      def to_s(io : IO) : Nil
        io << @expression
      end

      private def day_matches?(t : Time) : Bool
        dom_match = @dom.bit(t.day) == 1
        # Crystal: Monday=1..Sunday=7; cron: Sunday=0..Saturday=6
        crystal_dow = t.day_of_week.value
        cron_dow = crystal_dow == 7 ? 0 : crystal_dow
        dow_match = @dow.bit(cron_dow) == 1
        if @dom_restricted && @dow_restricted
          dom_match || dow_match
        elsif @dom_restricted
          dom_match
        elsif @dow_restricted
          dow_match
        else
          true
        end
      end

      private def beginning_of_next_month(t : Time) : Time
        if t.month == 12
          Time.utc(t.year + 1, 1, 1, 0, 0)
        else
          Time.utc(t.year, t.month + 1, 1, 0, 0)
        end
      end

      private def self.parse_field(s : String, lo : Int32, hi : Int32) : UInt64
        bits = 0_u64
        s.split(',').each do |part|
          raise ParseError.new("Empty field part") if part.empty?
          bits |= parse_part(part, lo, hi)
        end
        bits
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def self.parse_part(s : String, lo : Int32, hi : Int32) : UInt64
        range_str, step = if slash = s.index('/')
                            step_str = s[slash + 1..]
                            step_n = step_str.to_i? || raise ParseError.new("Invalid step #{step_str.inspect} in #{s.inspect}")
                            raise ParseError.new("Step must be > 0 in #{s.inspect}") if step_n < 1
                            {s[0...slash], step_n}
                          else
                            {s, 1}
                          end

        from, to = case range_str
                   when "*"
                     {lo, hi}
                   else
                     if dash = range_str.index('-')
                       a = range_str[0...dash].to_i? || raise ParseError.new("Invalid range bound in #{s.inspect}")
                       b = range_str[dash + 1..].to_i? || raise ParseError.new("Invalid range bound in #{s.inspect}")
                       {a, b}
                     else
                       raise ParseError.new("Step requires range or wildcard: #{s.inspect}") if step != 1
                       n = range_str.to_i? || raise ParseError.new("Invalid number in #{s.inspect}")
                       {n, n}
                     end
                   end

        if from < lo || to > hi || from > to
          raise ParseError.new("Value #{from}-#{to} out of range #{lo}-#{hi} in #{s.inspect}")
        end

        bits = 0_u64
        n = from
        while n <= to
          bits |= 1_u64 << n
          n += step
        end
        bits
      end
    end
  end
end

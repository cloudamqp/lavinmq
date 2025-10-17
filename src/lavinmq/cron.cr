require "time"

module LavinMQ
  # CRON expression parser and scheduler
  # Supports standard 5-field CRON format: minute hour day month weekday
  # Examples:
  #   "*/5 * * * *" - Every 5 minutes
  #   "0 12 * * *" - Every day at noon
  #   "0 0 1 * *" - First day of every month at midnight
  class Cron
    getter expression : String
    @fields : NamedTuple(minute: Array(Int32), hour: Array(Int32), day: Array(Int32), month: Array(Int32), weekday: Array(Int32))

    def initialize(@expression : String)
      @fields = parse(@expression)
    end

    # Calculate the next execution time after the given time
    def next(after : Time = Time.utc) : Time
      time = after.at_beginning_of_minute + 1.minute

      # Try up to 4 years in the future to find a match
      max_iterations = 366 * 24 * 60 * 4
      max_iterations.times do
        return time if matches?(time)
        time += 1.minute
      end

      raise "No valid CRON schedule found within 4 years"
    end

    # Check if the given time matches the CRON expression
    def matches?(time : Time) : Bool
      @fields[:minute].includes?(time.minute) &&
        @fields[:hour].includes?(time.hour) &&
        @fields[:day].includes?(time.day) &&
        @fields[:month].includes?(time.month) &&
        @fields[:weekday].includes?(time.day_of_week.value % 7)
    end

    private def parse(expression : String) : NamedTuple(minute: Array(Int32), hour: Array(Int32), day: Array(Int32), month: Array(Int32), weekday: Array(Int32))
      parts = expression.split(/\s+/)
      raise "Invalid CRON expression: expected 5 fields, got #{parts.size}" unless parts.size == 5

      {
        minute:  parse_field(parts[0], 0, 59),
        hour:    parse_field(parts[1], 0, 23),
        day:     parse_field(parts[2], 1, 31),
        month:   parse_field(parts[3], 1, 12),
        weekday: parse_field(parts[4], 0, 6),
      }
    end

    private def parse_field(field : String, min : Int32, max : Int32) : Array(Int32)
      return (min..max).to_a if field == "*"

      values = [] of Int32

      field.split(',').each do |part|
        if part.includes?('/')
          # Step values: */5 or 0-30/5
          range_part, step = part.split('/', 2)
          step_value = step.to_i

          if range_part == "*"
            (min..max).step(step_value).each { |v| values << v }
          else
            range_min, range_max = parse_range(range_part, min, max)
            (range_min..range_max).step(step_value).each { |v| values << v }
          end
        elsif part.includes?('-')
          # Range: 1-5
          range_min, range_max = parse_range(part, min, max)
          values.concat((range_min..range_max).to_a)
        else
          # Single value: 5
          value = part.to_i
          raise "Value #{value} out of range #{min}-#{max}" unless min <= value <= max
          values << value
        end
      end

      values.uniq.sort
    end

    private def parse_range(range : String, min : Int32, max : Int32) : Tuple(Int32, Int32)
      range_min, range_max = range.split('-', 2).map(&.to_i)
      raise "Range #{range_min}-#{range_max} out of bounds #{min}-#{max}" unless min <= range_min <= max && min <= range_max <= max
      {range_min, range_max}
    end
  end
end

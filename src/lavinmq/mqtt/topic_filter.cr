module LavinMQ
  module MQTT
    # Validation for MQTT topic filters.
    module TopicFilter
      extend self

      # Compiling walks one level per segment, so cap how deep a filter can be.
      MAX_FILTER_SEPARATORS = 200

      # A malformed filter would otherwise silently widen when compiled
      # (e.g. `secret/#/x` behaving as `secret/#`).
      def valid_filter?(filter : String) : Bool
        return false if filter.empty?
        levels = filter.split('/')
        last = levels.size - 1
        return false if last > MAX_FILTER_SEPARATORS
        levels.each_with_index do |level, i|
          if level.includes?('#')
            return false unless level == "#" && i == last
          elsif level.includes?('+')
            return false unless level == "+"
          end
        end
        true
      end
    end
  end
end

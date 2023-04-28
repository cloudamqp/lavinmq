require "log"
require "../../stdlib/log_entry"

module LavinMQ
  module Logging
    struct JournalLogFormat < Log::StaticFormatter
      def run
        source(after: " ")
        data(before: "[", after: "] ")
        message
        exception
      end
    end

    struct ExchangeLogFormat < Log::StaticFormatter
      def run
        source(after: " ")
        data(before: "[", after: "] ")
        message
      end
    end

    struct StdoutLogFormat < Log::StaticFormatter
      def run
        timestamp
        severity
        string " "
        source(after: " ")
        data(before: "[", after: "] ")
        fiber(before: "[", after: "] ")
        message
        exception
      end

      def fiber(before = nil, after = nil)
        return if @entry.fiber.nil?
        @io << before << @entry.fiber << after
      end

      def data(before = nil, after = nil, separator = " ")
        return if @entry.data.empty?
        @io << before
        found = false
        @entry.data.each do |k, v|
          @io << separator if found
          @io << k << "=" << v
          found = true
        end
        @io << after
      end
    end
  end
end

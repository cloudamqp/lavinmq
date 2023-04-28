require "log"
require "../../stdlib/log_entry"

module LavinMQ
  module Logging
    abstract struct StaticFormatter < ::Log::StaticFormatter
      def thread(before = nil, after = nil)
        {% if flag?(:preview_mt) %}
          @io << before << @entry.thread_id << after
        {% end %}
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

    struct JournalLogFormat < StaticFormatter
      def run
        source(after: " ")
        data(before: "[", after: "] ")
        message
        exception
      end
    end

    struct ExchangeLogFormat < StaticFormatter
      def run
        source(after: " ")
        data(before: "[", after: "] ")
        message
      end
    end

    struct StdoutLogFormat < StaticFormatter
      def run
        timestamp
        severity
        string " "
        thread(after: " ")
        source(after: " ")
        data(before: "[", after: "] ")
        fiber(before: "[", after: "] ")
        message
        exception
      end
    end
  end
end

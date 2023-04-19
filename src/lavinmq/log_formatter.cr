require "log"

module LavinMQ
  struct JournalLogFormat < Log::StaticFormatter
    def run
      source(after: " ")
      data(before: "[", after: "] ")
      message
      exception
    end
  end

  struct StdoutLogFormat < Log::StaticFormatter
    def run
      timestamp
      severity
      string " "
      source
      data
      string " "
      message
      exception
    end

    def data(before = "[", after = "]", seperator = " ")
      return if @entry.data.empty?
      @io << before
      found = false
      @entry.data.each do |k, v|
        @io << seperator if found
        @io << k << "=" << v
        found = true
      end
      @io << after
    end
  end
end

require "log"

module LavinMQ
  struct JournalLogFormat < Log::StaticFormatter
    def run
      source
      context(before: '[', after: ']')
      string ' '
      message
      exception
    end
  end

  struct StdoutLogFormat < Log::StaticFormatter
    def run
      timestamp
      severity
      source(before: ' ')
      context(before: '[', after: ']')
      string ' '
      message
      exception
    end
  end
end

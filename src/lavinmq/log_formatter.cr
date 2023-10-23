require "log"

module LavinMQ
  struct JournalLogFormat < Log::StaticFormatter
    def run
      source
      context(before: '[', after: ']')
      data(before: '[', after: ']')
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
      data(before: '[', after: ']')
      string ' '
      message
      exception
    end
  end
end

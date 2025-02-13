require "log"

module LavinMQ
  struct JournalLogFormat < ::Log::StaticFormatter
    def run
      @io << '<' << severity_to_priority << '>' << ' '
      source
      context(before: '[', after: ']')
      data(before: '[', after: ']')
      string ' '
      message
      exception
    end

    private def severity_to_priority
      case @entry.severity
      when ::Log::Severity::Trace  then 7 # journald doesn't have trace
      when ::Log::Severity::Debug  then 7
      when ::Log::Severity::Info   then 6
      when ::Log::Severity::Notice then 5
      when ::Log::Severity::Warn   then 4
      when ::Log::Severity::Error  then 3
      when ::Log::Severity::Fatal  then 2
      else                              6 # Default to "info"
      end
    end
  end

  struct StdoutLogFormat < ::Log::StaticFormatter
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

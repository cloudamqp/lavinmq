require "log"

module LavinMQ
  struct JournalLogFormat < ::Log::StaticFormatter
    def run
      severity
      source
      context(before: '[', after: ']')
      data(before: '[', after: ']')
      string ' '
      message
      exception
    end

    private def severity : Nil
      @io << '<' << severity_to_priority << '>' << ' '
    end

    private def exception(*, before = '\n', after = nil) : Nil
      if ex = @entry.exception
        @io << '\n'
        inspect_with_backtrace(ex)
        @io << after
      end
    end

    private def inspect_with_backtrace(ex : Exception) : Nil
      severity
      @io << message << " (" << ex.class << ")\n"

      ex.backtrace?.try &.each do |frame|
        severity
        @io.print "  from "
        @io.puts frame
      end

      if cause = ex.cause
        severity
        @io << "Caused by: "
        inspect_with_backtrace(cause)
      end

      @io.flush
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

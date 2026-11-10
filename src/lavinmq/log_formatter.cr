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

    private def inspect_with_backtrace(ex : Exception, prefix = "") : Nil
      severity
      # journald reads each line as its own entry, so escape line breaks in the message
      @io << prefix << ex.message.try(&.gsub({'\n' => "\\n", '\r' => "\\r"})) << " (" << ex.class << ")\n"

      ex.backtrace?.try &.each do |frame|
        severity
        @io.print "  from "
        @io.puts frame
      end

      if cause = ex.cause
        inspect_with_backtrace(cause, "Caused by: ")
      end

      @io.flush
    end

    private def severity_to_priority
      case @entry.severity
      in ::Log::Severity::Trace  then 7 # journald doesn't have trace
      in ::Log::Severity::Debug  then 7
      in ::Log::Severity::Info   then 6
      in ::Log::Severity::Notice then 5
      in ::Log::Severity::Warn   then 4
      in ::Log::Severity::Error  then 3
      in ::Log::Severity::Fatal  then 2
      in ::Log::Severity::None   then 6
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

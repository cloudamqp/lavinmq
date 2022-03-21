module AvalancheMQ
  class LogFormatter
    JOURNAL_STREAM = ENV.has_key?("JOURNAL_STREAM")

    def self.use(log : Logger, log_prefix_systemd_level = false)
      log.formatter = Logger::Formatter.new do |severity, datetime, progname, message, io|
        if log_prefix_systemd_level
          io << case severity
          when Logger::Severity::DEBUG then "<7>"
          when Logger::Severity::INFO  then "<6>"
          when Logger::Severity::WARN  then "<4>"
          when Logger::Severity::ERROR then "<3>"
          when Logger::Severity::FATAL then "<0>"
          else
          end
        end
        io << datetime << " [" << severity << "] " unless JOURNAL_STREAM
        io << progname << ": " << message
      end
    end
  end

  struct JournalLogFormat < Log::StaticFormatter
    def run
      source
      string ": "
      message
      data
      exception
    end
  end

  struct StdoutLogFormat < Log::StaticFormatter
    def run
      timestamp
      severity
      string " "
      source(after: " ")
      message
      data(before: " -- ")
      exception
    end
  end
end

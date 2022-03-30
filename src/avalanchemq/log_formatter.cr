module AvalancheMQ
  struct JournalLogFormat < Log::StaticFormatter
    def run
      source(after: " ")
      message
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
      exception
    end
  end
end

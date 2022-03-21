module AvalancheMQ
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

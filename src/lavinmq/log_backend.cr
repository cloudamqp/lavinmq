require "log"

module LavinMQ
  # Drops entries logged after close instead of raising in the logging fiber
  class LogBackend < ::Log::IOBackend
    @owns_io = false

    # Logs to the file at *path*, closed with the backend
    def initialize(path : String, *, formatter : ::Log::Formatter)
      @owns_io = true
      super(File.open(path, "a"), formatter: formatter)
    end

    def initialize(io : IO = STDOUT, *, formatter : ::Log::Formatter = ::Log::ShortFormat)
      super(io, formatter: formatter)
    end

    def dispatch(entry : ::Log::Entry) : Nil
      super
    rescue Channel::ClosedError
    end

    def close : Nil
      super
      io.close if @owns_io
    end
  end
end

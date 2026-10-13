require "log"

module LavinMQ
  # Drops entries logged after close instead of raising in the logging fiber
  class LogBackend < ::Log::IOBackend
    def dispatch(entry : ::Log::Entry) : Nil
      super
    rescue Channel::ClosedError
    end
  end
end
